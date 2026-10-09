//! HTTP Poll unit + real-network tests. Every network test uses an in-process
//! tokio HTTP/1.1 server on loopback; nothing leaves the host.

use super::*;
use crate::secret::MapSecretResolver;
use sparrow_model::{DataType, Field, FieldId, RowBatchBuilder, Scalar, SchemaId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

#[path = "mqtt/tls_fixture.rs"]
mod tls_fixture;

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

fn secrets() -> MapSecretResolver {
    MapSecretResolver::new(HashMap::from([
        ("tok".to_string(), "s3cr3t-token-value".to_string()),
        ("user".to_string(), "alice".to_string()),
        ("pass".to_string(), "pa55-word-value".to_string()),
        ("apikey".to_string(), "k3y-value-xyz".to_string()),
        ("bad".to_string(), "line\nbreak-secret".to_string()),
        ("colon".to_string(), "al:ice-secret".to_string()),
    ]))
}

fn cfg(url: &str) -> HttpPollSourceConfig {
    let mut c = HttpPollSourceConfig::new(url, schema());
    c.interval = Duration::from_millis(100);
    c.timeout = Duration::from_millis(1000);
    c.backoff_max = Duration::from_millis(800);
    c
}

fn allow(url: &str) -> TargetPolicy {
    let u = url::Url::parse(url).unwrap();
    TargetPolicy::allow(u.host_str().unwrap(), u.port_or_known_default().unwrap())
}

// ---------------------------------------------------------------- validation

type Mutation = Box<dyn Fn(&mut HttpPollSourceConfig)>;

#[test]
fn config_validation_bounds_and_policy() {
    let ok = "https://api.example:8443/v1/data?page=1";
    let s = secrets();
    let p = allow(ok);
    cfg(ok).validate(&s, &p).unwrap();
    let cases: Vec<(Mutation, ErrorCode)> = vec![
        (
            Box::new(|c| c.interval = Duration::from_millis(99)),
            ErrorCode::BoundExceeded,
        ),
        (
            Box::new(|c| c.timeout = Duration::from_secs(61)),
            ErrorCode::BoundExceeded,
        ),
        (
            Box::new(|c| c.backoff_max = Duration::from_millis(50)),
            ErrorCode::BoundExceeded,
        ),
        (
            Box::new(|c| c.max_response_bytes = HTTP_POLL_MAX_RESPONSE_BYTES + 1),
            ErrorCode::BoundExceeded,
        ),
        (Box::new(|c| c.inbox_capacity = 0), ErrorCode::BoundExceeded),
        (
            Box::new(|c| c.inbox_capacity = 4097),
            ErrorCode::BoundExceeded,
        ),
        (
            Box::new(|c| c.inbox_bytes = 4 * 1024 * 1024),
            ErrorCode::BoundExceeded,
        ),
        (
            Box::new(|c| c.json_limits.max_bytes = 128 * 1024),
            ErrorCode::BoundExceeded,
        ),
        (
            Box::new(|c| c.url = "https://u:p@api.example:8443/x".into()),
            ErrorCode::PolicyDenied,
        ),
        (
            Box::new(|c| c.url = "https://api.example:8443/x#frag".into()),
            ErrorCode::PolicyDenied,
        ),
        (
            Box::new(|c| c.url = "ftp://api.example:8443/x".into()),
            ErrorCode::InvalidArgument,
        ),
        (
            Box::new(|c| c.url = "https://other.example:8443/x".into()),
            ErrorCode::PolicyDenied,
        ),
        (
            Box::new(|c| c.url = "https://api.example:9999/x".into()),
            ErrorCode::PolicyDenied,
        ),
        (
            Box::new(|c| {
                c.restore = RestoreClaim::Checkpoint {
                    snapshot_id: "1".into(),
                }
            }),
            ErrorCode::UnsupportedRestore,
        ),
        (
            Box::new(|c| {
                c.auth = HttpPollAuth::Bearer {
                    token_secret: "missing".into(),
                }
            }),
            ErrorCode::SecretMissing,
        ),
        (
            Box::new(|c| {
                c.headers = vec![HttpPollHeader {
                    name: "Authorization".into(),
                    value: Some("x".into()),
                    value_secret: None,
                }]
            }),
            ErrorCode::InvalidArgument,
        ),
        (
            Box::new(|c| {
                c.headers = vec![HttpPollHeader {
                    name: "X-A".into(),
                    value: Some("x".into()),
                    value_secret: Some("tok".into()),
                }]
            }),
            ErrorCode::InvalidArgument,
        ),
        (
            Box::new(|c| {
                c.headers = vec![HttpPollHeader {
                    name: "X-A".into(),
                    value: None,
                    value_secret: None,
                }]
            }),
            ErrorCode::InvalidArgument,
        ),
        (
            Box::new(|c| {
                c.headers = vec![
                    HttpPollHeader {
                        name: "X-A".into(),
                        value: Some("1".into()),
                        value_secret: None,
                    },
                    HttpPollHeader {
                        name: "x-a".into(),
                        value: Some("2".into()),
                        value_secret: None,
                    },
                ]
            }),
            ErrorCode::InvalidArgument,
        ),
        (
            Box::new(|c| {
                c.headers = vec![HttpPollHeader {
                    name: "bad name".into(),
                    value: Some("x".into()),
                    value_secret: None,
                }]
            }),
            ErrorCode::InvalidArgument,
        ),
        (
            Box::new(|c| {
                c.headers = (0..17)
                    .map(|i| HttpPollHeader {
                        name: format!("x-{i}"),
                        value: Some("v".into()),
                        value_secret: None,
                    })
                    .collect()
            }),
            ErrorCode::BoundExceeded,
        ),
    ];
    for (i, (mutate, code)) in cases.iter().enumerate() {
        let mut c = cfg(ok);
        mutate(&mut c);
        let err = c
            .validate(&s, &p)
            .expect_err(&format!("case {i} must be rejected"));
        assert_eq!(err.code(), *code, "case {i}: {err}");
    }
    // Intervals above the 1h backoff ceiling stay valid: backoff_max may equal
    // the interval, but never exceed max(interval, 1h).
    let mut slow = cfg(ok);
    slow.interval = Duration::from_secs(2 * 3600);
    slow.backoff_max = slow.interval;
    slow.validate(&s, &p).unwrap();
    slow.backoff_max = Duration::from_secs(3 * 3600);
    assert_eq!(
        slow.validate(&s, &p).unwrap_err().code(),
        ErrorCode::BoundExceeded
    );
    let mut short = cfg(ok);
    short.backoff_max = Duration::from_secs(2 * 3600);
    assert_eq!(
        short.validate(&s, &p).unwrap_err().code(),
        ErrorCode::BoundExceeded
    );
    // Metadata / link-local targets are blocked even when allowlisted.
    let meta = "http://169.254.169.254/latest/meta-data";
    assert_eq!(
        cfg(meta).validate(&s, &allow(meta)).unwrap_err().code(),
        ErrorCode::PolicyDenied
    );
    // Deny-by-default.
    assert_eq!(
        cfg(ok)
            .validate(&s, &TargetPolicy::deny_all())
            .unwrap_err()
            .code(),
        ErrorCode::PolicyDenied
    );
}

#[test]
fn credentials_require_https_and_reservation_budget_is_checked() {
    let plain = "http://127.0.0.1:8080/data";
    let s = secrets();
    for auth in [
        HttpPollAuth::Bearer {
            token_secret: "tok".into(),
        },
        HttpPollAuth::Basic {
            username_secret: "user".into(),
            password_secret: "pass".into(),
        },
    ] {
        let mut c = cfg(plain);
        c.auth = auth;
        assert_eq!(
            c.validate(&s, &allow(plain)).unwrap_err().code(),
            ErrorCode::PolicyDenied
        );
    }
    let mut c = cfg(plain);
    c.headers = vec![HttpPollHeader {
        name: "X-Api-Key".into(),
        value: None,
        value_secret: Some("apikey".into()),
    }];
    assert_eq!(
        c.validate(&s, &allow(plain)).unwrap_err().code(),
        ErrorCode::PolicyDenied
    );
    // Non-secret literal headers are fine over plain HTTP.
    let mut c = cfg(plain);
    c.headers = vec![HttpPollHeader {
        name: "X-Tenant".into(),
        value: Some("t1".into()),
        value_secret: None,
    }];
    c.validate(&s, &allow(plain)).unwrap();
    let mut c = cfg(plain);
    c.max_response_bytes = HTTP_POLL_MAX_RESPONSE_BYTES;
    assert!(c.check_reservation_budget(4 * 1024 * 1024).is_ok());
    assert_eq!(
        c.check_reservation_budget(1024 * 1024).unwrap_err().code(),
        ErrorCode::BoundExceeded
    );
}

#[test]
fn auth_headers_are_built_sensitive_and_never_leak() {
    let url = "https://api.example:8443/v1";
    let s = secrets();
    let mut c = cfg(url);
    c.auth = HttpPollAuth::Basic {
        username_secret: "user".into(),
        password_secret: "pass".into(),
    };
    c.headers = vec![
        HttpPollHeader {
            name: "X-Api-Key".into(),
            value: None,
            value_secret: Some("apikey".into()),
        },
        HttpPollHeader {
            name: "X-Tenant".into(),
            value: Some("t1".into()),
            value_secret: None,
        },
        HttpPollHeader {
            name: "Accept".into(),
            value: Some("application/vnd.x+json".into()),
            value_secret: None,
        },
    ];
    let headers = build_headers(&c, &s).unwrap();
    let get = |n: &str| {
        headers
            .iter()
            .filter(|(k, _)| k.as_str() == n)
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
    };
    let auth = get("authorization");
    assert_eq!(auth.len(), 1);
    assert!(auth[0].is_sensitive());
    let expected = base64::engine::general_purpose::STANDARD.encode("alice:pa55-word-value");
    assert_eq!(auth[0].to_str().unwrap(), format!("Basic {expected}"));
    assert!(get("x-api-key")[0].is_sensitive());
    assert_eq!(get("x-api-key")[0].to_str().unwrap(), "k3y-value-xyz");
    assert!(!get("x-tenant")[0].is_sensitive());
    assert_eq!(get("accept").len(), 1, "user Accept replaces the default");

    c.auth = HttpPollAuth::Bearer {
        token_secret: "tok".into(),
    };
    let headers = build_headers(&c, &s).unwrap();
    let bearer = headers
        .iter()
        .find(|(k, _)| k == reqwest::header::AUTHORIZATION)
        .unwrap();
    assert_eq!(bearer.1.to_str().unwrap(), "Bearer s3cr3t-token-value");

    let source = HttpPollSource::bind(c.clone(), &s, &allow(url), IoDiagnostics::new()).unwrap();
    let debug = format!("{source:?} {:?} {:?}", source.config, source.headers);
    for secret in [
        "s3cr3t-token-value",
        "k3y-value-xyz",
        "pa55-word-value",
        &expected,
    ] {
        assert!(!debug.contains(secret), "Debug leaked a secret: {debug}");
    }
    // An invalid secret value is rejected without echoing it.
    c.auth = HttpPollAuth::Bearer {
        token_secret: "bad".into(),
    };
    let err = c.validate(&s, &allow(url)).unwrap_err();
    assert!(!err.to_string().contains("break-secret"), "{err}");
    c.auth = HttpPollAuth::Basic {
        username_secret: "colon".into(),
        password_secret: "pass".into(),
    };
    assert!(!c
        .validate(&s, &allow(url))
        .unwrap_err()
        .to_string()
        .contains("ice-secret"));
    c.auth = HttpPollAuth::None;
    c.headers = vec![HttpPollHeader {
        name: "X-K".into(),
        value: None,
        value_secret: Some("bad".into()),
    }];
    assert!(!c
        .validate(&s, &allow(url))
        .unwrap_err()
        .to_string()
        .contains("break-secret"));
}

// ------------------------------------------------------------------ splitter

fn spans(body: &str, format: HttpPollFormat) -> std::result::Result<Vec<String>, ErrorCode> {
    let mut r = Records::new(body.as_bytes(), format);
    let mut out = Vec::new();
    while let Some(s) = r.next_span() {
        let (a, b) = s.map_err(|e| e.code())?;
        out.push(body[a..b].to_string());
    }
    Ok(out)
}

#[test]
fn record_splitter_is_lazy_strict_and_string_aware() {
    use HttpPollFormat::*;
    assert_eq!(spans(" {\"a\":1} \n", Json).unwrap(), vec!["{\"a\":1}"]);
    assert_eq!(spans("", Json).unwrap(), Vec::<String>::new());
    assert_eq!(spans(" [ ] ", Json).unwrap(), Vec::<String>::new());
    assert_eq!(
        spans(r#"[{"a":"]},[{"},{"b":[1,{"c":2}]} , 3,"x\"]"]"#, Json).unwrap(),
        vec![r#"{"a":"]},[{"}"#, r#"{"b":[1,{"c":2}]}"#, "3", r#""x\"]""#]
    );
    for bad in [
        "[1,]",
        "[,1]",
        "[1 2]",
        "[{\"a\":1}",
        "[1] x",
        "\"str\"",
        "42",
        "[{\"a\":\"x]",
    ] {
        assert_eq!(spans(bad, Json), Err(ErrorCode::CodecViolation), "{bad}");
    }
    assert_eq!(
        spans("{\"a\":1}\r\n\n  {\"a\":2}  \n{\"a\":3}", Ndjson).unwrap(),
        vec!["{\"a\":1}", "{\"a\":2}", "{\"a\":3}"]
    );
    // 1 MiB of tiny elements: no index allocation, spans come one at a time.
    let many = format!("[{}0]", "0,".repeat(400_000));
    let mut r = Records::new(many.as_bytes(), Json);
    let mut n = 0;
    while let Some(s) = r.next_span() {
        s.unwrap();
        n += 1;
    }
    assert_eq!(n, 400_001);
}

// --------------------------------------------------------- test HTTP server

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    content_length: bool,
    delay: Duration,
}

impl Reply {
    fn json(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: vec![],
            body: body.into(),
            content_length: true,
            delay: Duration::ZERO,
        }
    }
    fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::json("{}")
        }
    }
}

type Handler = Arc<dyn Fn(usize, &str) -> Reply + Send + Sync>;

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    max_active: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(handler: Handler) -> Self {
        Self::start_inner(handler, None).await
    }

    async fn start_tls(handler: Handler) -> Self {
        use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap()],
                PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap(),
            )
            .unwrap();
        Self::start_inner(
            handler,
            Some(tokio_rustls::TlsAcceptor::from(Arc::new(config))),
        )
        .await
    }

    async fn start_inner(handler: Handler, tls: Option<tokio_rustls::TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let (r, a, m) = (requests.clone(), active.clone(), max_active.clone());
        let secure = tls.is_some();
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let (handler, r, a, m, tls) = (
                    handler.clone(),
                    r.clone(),
                    a.clone(),
                    m.clone(),
                    tls.clone(),
                );
                tokio::spawn(async move {
                    match tls {
                        Some(acceptor) => {
                            if let Ok(stream) = acceptor.accept(socket).await {
                                serve(stream, handler, r, a, m).await;
                            }
                        }
                        None => serve(socket, handler, r, a, m).await,
                    }
                });
            }
        });
        let scheme = if secure { "https" } else { "http" };
        let host = if secure { "localhost" } else { "127.0.0.1" };
        Self {
            url: format!("{scheme}://{host}:{port}/data"),
            requests,
            max_active,
            task,
        }
    }

    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    handler: Handler,
    requests: Arc<Mutex<Vec<String>>>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte).await {
            Ok(1) => head.push(byte[0]),
            _ => return,
        }
        if head.len() > 16 * 1024 {
            return;
        }
    }
    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
    max_active.fetch_max(now, Ordering::SeqCst);
    let head = String::from_utf8_lossy(&head).into_owned();
    let index = {
        let mut r = requests.lock().unwrap();
        r.push(head.clone());
        r.len() - 1
    };
    let reply = handler(index, &head);
    tokio::time::sleep(reply.delay).await;
    let mut out = format!("HTTP/1.1 {} X\r\nconnection: close\r\n", reply.status);
    if reply.content_length {
        out.push_str(&format!("content-length: {}\r\n", reply.body.len()));
    }
    for (k, v) in &reply.headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let _ = stream.write_all(out.as_bytes()).await;
    let _ = stream.write_all(&reply.body).await;
    let _ = stream.shutdown().await;
    active.fetch_sub(1, Ordering::SeqCst);
}

// ----------------------------------------------------------- run harness

struct Running {
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    rx: sparrow_io::observed::Receiver<QueuedRow>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<Result<()>>,
}

fn start(config: HttpPollSourceConfig, client: Option<reqwest::Client>) -> Running {
    let diag = IoDiagnostics::new();
    let policy = allow(&config.url);
    let capacity = config.inbox_capacity;
    let mut source = HttpPollSource::bind(config, &secrets(), &policy, diag.clone()).unwrap();
    if let Some(client) = client {
        // Test-only trust anchor; verification stays on (no skip-verify).
        source.client = client;
    }
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (tx, rx) = sparrow_io::observed::channel(capacity);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(source.run_budgeted(tx, cancel.clone(), owner.clone(), 64 * 1024));
    Running {
        diag,
        owner,
        rx,
        cancel,
        task,
    }
}

impl Running {
    fn rows_from(&self, queued: Vec<QueuedRow>) -> Vec<Vec<Scalar>> {
        let mut builder = RowBatchBuilder::new(
            Arc::new(schema()),
            self.owner.clone(),
            CreditKind::Reservation,
            queued.len().max(1),
            1 << 20,
        )
        .unwrap();
        for q in queued {
            q.push_into(&mut builder).unwrap();
        }
        builder
            .finish()
            .unwrap()
            .rows()
            .iter()
            .map(|r| r.values.clone())
            .collect()
    }

    async fn take(&mut self, n: usize) -> Vec<Vec<Scalar>> {
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while got.len() < n {
                got.push(self.rx.recv().await.expect("source closed"));
            }
        })
        .await
        .expect("rows deadline");
        self.rows_from(got)
    }

    async fn stop(self) -> Result<()> {
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .expect("HTTP poll source must stop promptly")
            .unwrap()
    }
}

fn row(id: &str, v: i64) -> Vec<Scalar> {
    vec![Scalar::utf8(id), Scalar::Int64(v)]
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

// ----------------------------------------------------- real network tests

#[tokio::test]
async fn periodic_ingest_json_array_object_and_ndjson() {
    let server = Server::start(Arc::new(|i, _| {
        Reply::json(format!(
            r#"[{{"device_id":"d","v":{}}},{{"device_id":"e","v":{}}}]"#,
            2 * i,
            2 * i + 1
        ))
    }))
    .await;
    let mut run = start(cfg(&server.url), None);
    let rows = run.take(6).await;
    assert_eq!(
        rows,
        vec![
            row("d", 0),
            row("e", 1),
            row("d", 2),
            row("e", 3),
            row("d", 4),
            row("e", 5)
        ]
    );
    assert!(server.count() >= 3);
    assert_eq!(server.max_active.load(Ordering::SeqCst), 1);
    assert!(server.requests.lock().unwrap()[0].starts_with("GET /data HTTP/1.1"));
    let snap = run.diag.snapshot();
    assert!(
        snap.http_poll_ok >= 3 && snap.http_poll_rows >= 6,
        "{snap:?}"
    );
    run.stop().await.unwrap();

    let server = Server::start(Arc::new(|i, _| {
        Reply::json(format!(r#"{{"device_id":"o","v":{i}}}"#))
    }))
    .await;
    let mut run = start(cfg(&server.url), None);
    assert_eq!(run.take(2).await, vec![row("o", 0), row("o", 1)]);
    run.stop().await.unwrap();

    let server = Server::start(Arc::new(|_, _| {
        Reply::json("{\"device_id\":\"n\",\"v\":1}\n{\"device_id\":\"n\",\"v\":\"bad\"}\n\n{\"device_id\":\"n\",\"v\":2}\n")
    }))
    .await;
    let mut c = cfg(&server.url);
    c.format = HttpPollFormat::Ndjson;
    let mut run = start(c, None);
    assert_eq!(run.take(2).await, vec![row("n", 1), row("n", 2)]);
    assert!(run.diag.snapshot().http_poll_dropped_bad >= 1);
    assert!(server.requests.lock().unwrap()[0]
        .to_ascii_lowercase()
        .contains("accept: application/x-ndjson"));
    run.stop().await.unwrap();
}

#[tokio::test]
async fn oversized_response_is_rejected_with_and_without_content_length() {
    for content_length in [true, false] {
        let big = format!(r#"[{{"device_id":"{}","v":1}}]"#, "x".repeat(4000));
        let server = Server::start(Arc::new(move |_, _| Reply {
            content_length,
            ..Reply::json(big.clone())
        }))
        .await;
        let mut c = cfg(&server.url);
        c.max_response_bytes = 1024;
        let run = start(c, None);
        until(Duration::from_secs(3), || {
            run.diag.snapshot().http_poll_oversize >= 2
        })
        .await;
        let snap = run.diag.snapshot();
        assert_eq!(
            snap.http_poll_rows, 0,
            "no partial rows from an oversized body"
        );
        assert!(snap.http_poll_failed >= 2);
        // Body buffer never exceeded the cap's billing.
        assert!(run.owner.usage().reservation_bytes <= 1024);
        run.stop().await.unwrap();
    }
}

#[tokio::test]
async fn request_timeout_is_bounded_and_counted() {
    let server = Server::start(Arc::new(|_, _| Reply {
        delay: Duration::from_secs(5),
        ..Reply::json("[]")
    }))
    .await;
    let mut c = cfg(&server.url);
    c.timeout = Duration::from_millis(100);
    let run = start(c, None);
    until(Duration::from_secs(3), || {
        run.diag.snapshot().http_poll_failed >= 1
    })
    .await;
    let snap = run.diag.snapshot();
    assert_eq!(snap.http_poll_rows, 0);
    assert_eq!(snap.http_poll_inflight, 0);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn status_errors_back_off_exponentially_then_recover() {
    let healthy = Arc::new(AtomicBool::new(false));
    let h = healthy.clone();
    let server = Server::start(Arc::new(move |i, _| {
        if h.load(Ordering::SeqCst) {
            Reply::json(format!(r#"{{"device_id":"r","v":{i}}}"#))
        } else if i % 2 == 0 {
            Reply::status(500)
        } else {
            Reply::status(401)
        }
    }))
    .await;
    let mut run = start(cfg(&server.url), None);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    // interval=100ms, backoff 200,400,800(cap),800: ~0,200,600,1400ms.
    let failing = server.count();
    assert!(
        (3..=6).contains(&failing),
        "backoff must slow polling: {failing} requests"
    );
    let snap = run.diag.snapshot();
    assert_eq!(snap.http_poll_status_errors as usize, failing);
    assert_eq!(snap.http_poll_rows, 0);
    healthy.store(true, Ordering::SeqCst);
    let rows = run.take(3).await;
    assert_eq!(rows.len(), 3);
    // After recovery the interval (not the backoff) applies again.
    let before = server.count();
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert!(
        server.count() >= before + 2,
        "interval must reset after success"
    );
    run.stop().await.unwrap();
}

#[tokio::test]
async fn slow_consumer_skips_ticks_without_overlap_or_memory_growth() {
    let server = Server::start(Arc::new(|_, _| {
        let rows: Vec<String> = (0..10)
            .map(|v| format!(r#"{{"device_id":"s","v":{v}}}"#))
            .collect();
        Reply::json(format!("[{}]", rows.join(",")))
    }))
    .await;
    let mut c = cfg(&server.url);
    c.inbox_capacity = 2;
    let mut run = start(c, None);
    tokio::time::sleep(Duration::from_millis(900)).await;
    // Inbox full after 2 rows; the third waits; no new request is issued.
    assert_eq!(
        server.count(),
        1,
        "no poll while the previous batch is not admitted"
    );
    let snap = run.diag.snapshot();
    assert_eq!(snap.http_poll_rows, 2);
    assert!(snap.http_poll_inbox_items <= 2);
    assert_eq!(snap.http_poll_backpressure_waits, 1);
    let queue = run.owner.usage().queue_bytes;
    let reservation = run.owner.usage().reservation_bytes;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        run.owner.usage().queue_bytes,
        queue,
        "held queue credit must not grow"
    );
    assert_eq!(run.owner.usage().reservation_bytes, reservation);
    // Drain: the first response completes, its long busy period is recorded
    // as skipped ticks, then polling resumes.
    let rows = run.take(12).await;
    assert_eq!(
        rows[..10].iter().map(|r| r[1].clone()).collect::<Vec<_>>(),
        (0..10).map(Scalar::Int64).collect::<Vec<_>>()
    );
    assert!(
        run.diag.snapshot().http_poll_skipped_ticks >= 10,
        "{:?}",
        run.diag.snapshot()
    );
    assert!(server.count() >= 2);
    assert_eq!(server.max_active.load(Ordering::SeqCst), 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn stop_while_blocked_on_admission_is_prompt_and_releases_credit() {
    let server = Server::start(Arc::new(|_, _| {
        Reply::json(r#"[{"device_id":"a","v":1},{"device_id":"a","v":2}]"#)
    }))
    .await;
    let mut c = cfg(&server.url);
    c.inbox_capacity = 1;
    let run = start(c, None);
    until(Duration::from_secs(3), || {
        run.diag.snapshot().http_poll_backpressure_waits == 1
    })
    .await;
    let owner = run.owner.clone();
    assert!(
        owner.usage().reservation_bytes > 0,
        "body + working row are billed while blocked"
    );
    run.stop().await.unwrap();
    // Body, working row and the queued row (receiver dropped) are all refunded.
    let usage = owner.usage();
    assert_eq!((usage.reservation_bytes, usage.queue_bytes), (0, 0));
}

#[tokio::test]
async fn https_bearer_basic_and_secret_headers_reach_the_server() {
    let server =
        Server::start_tls(Arc::new(|_, _| Reply::json(r#"{"device_id":"t","v":7}"#))).await;
    let mut c = cfg(&server.url);
    c.auth = HttpPollAuth::Bearer {
        token_secret: "tok".into(),
    };
    c.headers = vec![
        HttpPollHeader {
            name: "X-Api-Key".into(),
            value: None,
            value_secret: Some("apikey".into()),
        },
        HttpPollHeader {
            name: "X-Tenant".into(),
            value: Some("t1".into()),
            value_secret: None,
        },
    ];
    let trusted = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .add_root_certificate(reqwest::Certificate::from_pem(tls_fixture::CA).unwrap())
        .build()
        .unwrap();
    let mut run = start(c.clone(), Some(trusted.clone()));
    assert_eq!(run.take(1).await, vec![row("t", 7)]);
    let head = server.requests.lock().unwrap()[0].to_ascii_lowercase();
    assert!(
        head.contains("authorization: bearer s3cr3t-token-value"),
        "{head}"
    );
    assert!(head.contains("x-api-key: k3y-value-xyz"));
    assert!(head.contains("x-tenant: t1"));
    run.stop().await.unwrap();

    c.auth = HttpPollAuth::Basic {
        username_secret: "user".into(),
        password_secret: "pass".into(),
    };
    let mut run = start(c.clone(), Some(trusted));
    run.take(1).await;
    let expected = base64::engine::general_purpose::STANDARD
        .encode("alice:pa55-word-value")
        .to_ascii_lowercase();
    assert!(server
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .to_ascii_lowercase()
        .contains(&format!("authorization: basic {expected}")));
    run.stop().await.unwrap();

    // Default trust (webpki roots only) rejects the fixture: verification is on.
    let run = start(c, None);
    until(Duration::from_secs(3), || {
        run.diag.snapshot().http_poll_failed >= 1
    })
    .await;
    assert_eq!(run.diag.snapshot().http_poll_rows, 0);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn conditional_requests_send_validators_and_skip_304() {
    let server = Server::start(Arc::new(|i, head| {
        let lower = head.to_ascii_lowercase();
        if i > 0
            && lower.contains("if-none-match: \"v1\"")
            && lower.contains("if-modified-since: wed, 21 oct 2015 07:28:00 gmt")
        {
            Reply {
                body: vec![],
                ..Reply::status(304)
            }
        } else {
            Reply {
                headers: vec![
                    ("etag".into(), "\"v1\"".into()),
                    (
                        "last-modified".into(),
                        "Wed, 21 Oct 2015 07:28:00 GMT".into(),
                    ),
                ],
                ..Reply::json(r#"[{"device_id":"c","v":1}]"#)
            }
        }
    }))
    .await;
    let mut c = cfg(&server.url);
    c.conditional = true;
    let mut run = start(c, None);
    assert_eq!(run.take(1).await, vec![row("c", 1)]);
    until(Duration::from_secs(3), || {
        run.diag.snapshot().http_poll_not_modified >= 2
    })
    .await;
    assert!(run.rx.try_recv().is_err(), "304 must not re-emit rows");
    assert_eq!(run.diag.snapshot().http_poll_rows, 1);
    run.stop().await.unwrap();
}

#[tokio::test]
async fn fail_on_decode_fails_the_source_instead_of_dropping() {
    let server = Server::start(Arc::new(|_, _| {
        Reply::json(r#"[{"device_id":"f","v":"nope"}]"#)
    }))
    .await;
    let mut c = cfg(&server.url);
    c.fail_on_decode = true;
    let run = start(c, None);
    let result = tokio::time::timeout(Duration::from_secs(3), run.task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    let server = Server::start(Arc::new(|_, _| Reply::json("not json at all"))).await;
    let mut c = cfg(&server.url);
    c.fail_on_decode = true;
    let run = start(c, None);
    let err = tokio::time::timeout(Duration::from_secs(3), run.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::CodecViolation);
    assert_eq!(run.diag.snapshot().http_poll_bad_responses, 1);
}

fn csv_cfg(url: &str, options: sparrow_formats::CsvOptions) -> HttpPollSourceConfig {
    let mut c = cfg(url);
    c.payload_format =
        PayloadFormat::csv(options.compile(sparrow_formats::CsvRole::Decode).unwrap());
    c
}

#[tokio::test]
async fn csv_documents_map_headers_drop_bad_records_and_reject_bad_headers() {
    let server = Server::start(Arc::new(|i, _| match i {
        // BOM, CRLF, reordered header, quoted delimiter, a bad record.
        0 => Reply::json("\u{feff}v,device_id\r\n1,\"a,b\"\r\nnope,x\r\n2,c\r\n"),
        // Empty body: no rows, not an error.
        1 => Reply::json(""),
        // A header missing a column rejects the whole response.
        2 => Reply::json("device_id\nz\n"),
        _ => Reply::json(format!("device_id,v\nd,{i}\n")),
    }))
    .await;
    let mut run = start(csv_cfg(&server.url, Default::default()), None);
    let rows = run.take(3).await;
    assert_eq!(rows, vec![row("a,b", 1), row("c", 2), row("d", 3)]);
    let snap = run.diag.snapshot();
    assert_eq!(snap.http_poll_dropped_bad, 1, "{snap:?}");
    assert_eq!(snap.csv_type_errors, 1);
    assert_eq!(snap.csv_header_errors, 1);
    assert_eq!(snap.http_poll_bad_responses, 1);
    assert!(server.requests.lock().unwrap()[0]
        .to_ascii_lowercase()
        .contains("accept: text/csv"));
    run.stop().await.unwrap();

    // Headerless with explicit columns and a custom delimiter.
    let server = Server::start(Arc::new(|_, _| Reply::json("q|7\n"))).await;
    let options = sparrow_formats::CsvOptions {
        header: false,
        delimiter: "|".into(),
        columns: Some(vec!["device_id".into(), "v".into()]),
        ..Default::default()
    };
    let mut run = start(csv_cfg(&server.url, options), None);
    assert_eq!(run.take(1).await, vec![row("q", 7)]);
    run.stop().await.unwrap();

    // fail_on_decode: a bad record or header fails the source.
    for body in ["device_id,v\nd,bad\n", "x,y\n1,2\n"] {
        let server = Server::start(Arc::new(move |_, _| Reply::json(body))).await;
        let mut c = csv_cfg(&server.url, Default::default());
        c.fail_on_decode = true;
        let run = start(c, None);
        let result = tokio::time::timeout(Duration::from_secs(3), run.task)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err(), "{body}");
    }
}

#[test]
fn csv_refuses_ndjson_framing_and_unfit_schema() {
    let url = "https://api.example:8443/v1";
    let mut c = csv_cfg(url, Default::default());
    c.validate(&secrets(), &allow(url)).unwrap();
    c.format = HttpPollFormat::Ndjson;
    assert_eq!(
        c.validate(&secrets(), &allow(url)).unwrap_err().code(),
        ErrorCode::InvalidArgument
    );
    let mut c = csv_cfg(url, Default::default());
    c.schema = Schema::new(
        SchemaId::new(1),
        vec![Field::new(FieldId::new(1), "v", DataType::Int64, true)],
    )
    .unwrap();
    assert!(c.validate(&secrets(), &allow(url)).is_err());
}

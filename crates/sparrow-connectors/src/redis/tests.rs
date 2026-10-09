//! In-process RESP2 mock (plain TCP or TLS) for the Redis Sink and Lookup.
//! Real-server coverage lives in `real_tests` (opt-in, `SPARROW_REDIS_SERVER`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, InflightCounter, MemoryOwner, ResourceBudget,
    Row, RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::tls_fixture;
use super::*;
use crate::diag::IoDiagnostics;
use crate::{MapSecretResolver, TargetPolicy};

// ---------------------------------------------------------------- mock --

#[derive(Clone, Debug)]
pub(super) enum Act {
    Reply(Vec<u8>),
    /// Reply, then close the connection.
    ReplyClose(Vec<u8>),
    /// Close without replying (command may have been "executed").
    Close,
    /// Never reply.
    Hang,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Ctx {
    pub conn: usize,
    /// Commands seen before this one, on any connection.
    pub seen: usize,
}

type CommandLog = Arc<Mutex<Vec<(usize, Vec<Vec<u8>>)>>>;
type Mutation<T> = Box<dyn Fn(&mut T)>;
type Script = Arc<dyn Fn(Ctx, &[Vec<u8>]) -> Act + Send + Sync>;

pub(super) struct Mock {
    pub port: u16,
    /// Every command, as (connection, args).
    pub log: CommandLog,
    /// Raw request bytes per connection.
    pub raw: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    pub conns: Arc<AtomicUsize>,
}

impl Mock {
    pub async fn start(
        tls: bool,
        script: impl Fn(Ctx, &[Vec<u8>]) -> Act + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let raw = Arc::new(Mutex::new(HashMap::new()));
        let conns = Arc::new(AtomicUsize::new(0));
        let script: Script = Arc::new(script);
        let acceptor = tls.then(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap()],
                    PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap(),
                )
                .unwrap();
            tokio_rustls::TlsAcceptor::from(Arc::new(config))
        });
        let (l, r, c) = (log.clone(), raw.clone(), conns.clone());
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let id = c.fetch_add(1, Ordering::SeqCst);
                let (l, r, script) = (l.clone(), r.clone(), script.clone());
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    match acceptor {
                        None => serve(tcp, id, l, r, script).await,
                        Some(a) => {
                            if let Ok(s) = a.accept(tcp).await {
                                serve(s, id, l, r, script).await
                            }
                        }
                    }
                });
            }
        });
        Self {
            port,
            log,
            raw,
            conns,
        }
    }

    pub fn commands(&self) -> Vec<Vec<String>> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|(_, args)| {
                args.iter()
                    .map(|a| String::from_utf8_lossy(a).to_string())
                    .collect()
            })
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.commands().into_iter().map(|c| c[0].clone()).collect()
    }

    pub fn raw_all(&self) -> Vec<u8> {
        let raw = self.raw.lock().unwrap();
        let mut ids: Vec<_> = raw.keys().copied().collect();
        ids.sort_unstable();
        ids.into_iter().flat_map(|i| raw[&i].clone()).collect()
    }
}

/// Parse complete `*n $len arg ...` commands from `buf`.
fn take_command(buf: &mut Vec<u8>) -> Option<Vec<Vec<u8>>> {
    fn line(buf: &[u8], at: usize) -> Option<(usize, usize)> {
        let end = buf[at..].windows(2).position(|w| w == b"\r\n")? + at;
        let n: usize = std::str::from_utf8(&buf[at + 1..end]).ok()?.parse().ok()?;
        Some((n, end + 2))
    }
    if buf.first() != Some(&b'*') {
        return None;
    }
    let (n, mut at) = line(buf, 0)?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        if buf.get(at) != Some(&b'$') {
            return None;
        }
        let (len, start) = line(buf, at)?;
        if buf.len() < start + len + 2 {
            return None;
        }
        args.push(buf[start..start + len].to_vec());
        at = start + len + 2;
    }
    buf.drain(..at);
    Some(args)
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut s: S,
    id: usize,
    log: CommandLog,
    raw: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    script: Script,
) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        while let Some(args) = take_command(&mut buf) {
            let seen = {
                let mut log = log.lock().unwrap();
                log.push((id, args.clone()));
                log.len() - 1
            };
            match script(Ctx { conn: id, seen }, &args) {
                Act::Reply(bytes) => {
                    if s.write_all(&bytes).await.is_err() {
                        return;
                    }
                    let _ = s.flush().await;
                }
                Act::ReplyClose(bytes) => {
                    let _ = s.write_all(&bytes).await;
                    let _ = s.flush().await;
                    let _ = s.shutdown().await;
                    return;
                }
                Act::Close => {
                    let _ = s.shutdown().await;
                    return;
                }
                Act::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    return;
                }
            }
        }
        let n = match s.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        raw.lock()
            .unwrap()
            .entry(id)
            .or_default()
            .extend_from_slice(&chunk[..n]);
        buf.extend_from_slice(&chunk[..n]);
    }
}

pub(super) fn ok() -> Act {
    Act::Reply(b"+OK\r\n".to_vec())
}

pub(super) fn int(n: i64) -> Act {
    Act::Reply(format!(":{n}\r\n").into_bytes())
}

pub(super) fn bulk(b: &[u8]) -> Act {
    let mut v = format!("${}\r\n", b.len()).into_bytes();
    v.extend_from_slice(b);
    v.extend_from_slice(b"\r\n");
    Act::Reply(v)
}

/// A well-behaved server for the Sink commands.
pub(super) fn redis_like(args: &[Vec<u8>]) -> Act {
    match args[0].as_slice() {
        b"AUTH" if args.last().unwrap() == b"hunter2" => ok(),
        b"AUTH" => Act::Reply(b"-WRONGPASS invalid username-password pair\r\n".to_vec()),
        b"SELECT" | b"SET" => ok(),
        b"HSET" => int(((args.len() - 2) / 2) as i64),
        b"XADD" => bulk(b"1700000000000-0"),
        b"PUBLISH" => int(0),
        b"LPUSH" | b"RPUSH" => int(1),
        _ => Act::Reply(b"-ERR unknown command\r\n".to_vec()),
    }
}

// ------------------------------------------------------------- harness --

pub(super) fn schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "id", DataType::Utf8, true),
            Field::new(FieldId::new(2), "n", DataType::Int64, true),
            Field::new(FieldId::new(3), "v", DataType::Float64, true),
            Field::new(FieldId::new(4), "ok", DataType::Bool, true),
        ],
    )
    .unwrap()
}

pub(super) fn row(id: &str, n: i64, v: f64) -> Row {
    Row {
        values: vec![
            Scalar::utf8(id),
            Scalar::Int64(n),
            Scalar::Float64(v),
            Scalar::Bool(true),
        ],
    }
}

pub(super) fn batch(owner: &Arc<MemoryOwner>, rows: Vec<Row>) -> RowBatch {
    let mut b = RowBatchBuilder::new(
        Arc::new(schema()),
        owner.clone(),
        CreditKind::Reservation,
        rows.len().max(1),
        1 << 20,
    )
    .unwrap();
    for r in rows {
        b.push(r).unwrap();
    }
    b.finish().unwrap()
}

pub(super) fn tpl(s: &str) -> Template {
    Template::parse(s).unwrap()
}

pub(super) fn set_cmd() -> RedisCommand {
    RedisCommand::Set {
        key: tpl("dev:{id}"),
        value: RedisValue::Column("n".into()),
        ttl: None,
    }
}

pub(super) fn config(port: u16, command: RedisCommand) -> RedisSinkConfig {
    let mut c = RedisSinkConfig::new(
        RedisTarget::new(format!("redis://127.0.0.1:{port}")),
        command,
    );
    c.flush_interval = Duration::from_millis(5);
    c.retry_initial = Duration::from_millis(10);
    c.retry_max = Duration::from_millis(100);
    c.timeout = Duration::from_millis(1000);
    c.flush_timeout = Duration::from_millis(300);
    c
}

pub(super) fn secrets() -> MapSecretResolver {
    MapSecretResolver::new(
        [("pw", "hunter2"), ("user", "app"), ("bad", "nope")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

pub(super) fn owner() -> Arc<MemoryOwner> {
    MemoryOwner::new(ResourceBudget::compact())
}

pub(super) struct Harness {
    tx: Option<sparrow_io::observed::Sender<RowBatch>>,
    pub outbox: Arc<InflightCounter>,
    pub cancel: CancellationToken,
    pub diag: Arc<IoDiagnostics>,
    pub owner: Arc<MemoryOwner>,
    task: tokio::task::JoinHandle<()>,
}

impl Harness {
    pub fn start(config: RedisSinkConfig, owner: Arc<MemoryOwner>) -> Self {
        let ep = config.target.endpoint().unwrap();
        let diag = IoDiagnostics::new();
        diag.observation.initialize(&owner).unwrap();
        let sink = RedisSink::bind(
            config,
            &secrets(),
            &TargetPolicy::allow(ep.host.clone(), ep.port),
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

    pub async fn send(&self, rows: Vec<Row>) {
        self.send_batch(batch(&self.owner, rows)).await
    }

    pub async fn send_batch(&self, b: RowBatch) {
        self.outbox.enqueue();
        self.tx.as_ref().unwrap().send(b).await.unwrap();
    }

    pub async fn settled(&self, n: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.outbox.acked() + self.outbox.failed() < n {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("receipts settle");
    }

    /// EOF and wait for the Sink to finish.
    pub async fn finish(mut self) -> (Arc<InflightCounter>, crate::diag::IoSnapshot) {
        drop(self.tx.take());
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .expect("sink finishes")
            .unwrap();
        (self.outbox.clone(), self.diag.snapshot())
    }
}

fn resp(args: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    resp::push_command(
        &mut out,
        &args.iter().map(|a| a.as_bytes()).collect::<Vec<_>>(),
    );
    out
}

// ---------------------------------------------------------- sink tests --

#[tokio::test]
async fn every_command_writes_exact_resp_and_acks_on_its_reply() {
    // The shared JSON row encoder emits members in name order.
    let json_schema_row = r#"{"id":"a","n":1,"ok":true,"v":0.5}"#;
    let cases: Vec<(RedisCommand, Vec<u8>)> = vec![
        (
            RedisCommand::Set {
                key: tpl("dev:{id}:{{n}}"),
                value: RedisValue::Column("v".into()),
                ttl: Some(Duration::from_millis(1500)),
            },
            resp(&["SET", "dev:a:{n}", "0.5", "PX", "1500"]),
        ),
        (
            RedisCommand::Hset {
                key: tpl("h:{id}"),
                fields: HashFields::Columns(vec!["n".into(), "v".into(), "ok".into()]),
            },
            resp(&["HSET", "h:a", "n", "1", "v", "0.5", "ok", "true"]),
        ),
        (
            RedisCommand::Hset {
                key: tpl("latest"),
                fields: HashFields::Field {
                    field: tpl("{id}"),
                    value: RedisValue::Json,
                },
            },
            resp(&["HSET", "latest", "a", json_schema_row]),
        ),
        (
            RedisCommand::Xadd {
                key: tpl("s:{id}"),
                fields: vec!["n".into(), "v".into()],
                maxlen: Some(1000),
                approximate: true,
            },
            resp(&[
                "XADD", "s:a", "MAXLEN", "~", "1000", "*", "n", "1", "v", "0.5",
            ]),
        ),
        (
            RedisCommand::Xadd {
                key: tpl("s"),
                fields: vec!["n".into()],
                maxlen: Some(5),
                approximate: false,
            },
            resp(&["XADD", "s", "MAXLEN", "=", "5", "*", "n", "1"]),
        ),
        (
            RedisCommand::Publish {
                channel: tpl("ch.{id}"),
                value: RedisValue::Json,
            },
            resp(&["PUBLISH", "ch.a", json_schema_row]),
        ),
        (
            RedisCommand::Push {
                key: tpl("q"),
                value: RedisValue::Column("id".into()),
                left: true,
            },
            resp(&["LPUSH", "q", "a"]),
        ),
        (
            RedisCommand::Push {
                key: tpl("q"),
                value: RedisValue::Column("n".into()),
                left: false,
            },
            resp(&["RPUSH", "q", "1"]),
        ),
    ];
    for (command, expected) in cases {
        let name = command.name();
        let mock = Mock::start(false, |_, a| redis_like(a)).await;
        let h = Harness::start(config(mock.port, command), owner());
        h.send(vec![row("a", 1, 0.5)]).await;
        let (outbox, snap) = h.finish().await;
        assert_eq!(mock.raw_all(), expected, "{name}");
        assert_eq!((outbox.acked(), outbox.failed()), (1, 0), "{name}");
        assert_eq!(snap.redis_sink_commands_ok, 1, "{name}");
        assert_eq!(
            snap.redis_sink_publish_no_receivers,
            u64::from(name == "PUBLISH")
        );
    }
}

#[tokio::test]
async fn pipelines_are_bounded_by_rows_and_bytes_and_null_fields_are_skipped() {
    let mock = Mock::start(false, |_, a| redis_like(a)).await;
    let mut c = config(
        mock.port,
        RedisCommand::Hset {
            key: tpl("h:{id}"),
            fields: HashFields::Columns(vec!["n".into(), "v".into()]),
        },
    );
    c.pipeline_rows = 3;
    let h = Harness::start(c, owner());
    let mut rows: Vec<Row> = (0..7).map(|i| row(&format!("r{i}"), i, 1.0)).collect();
    rows[1].values[2] = Scalar::Null;
    h.send(rows).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(outbox.acked(), 1);
    assert_eq!(snap.redis_sink_pipelines, 3, "3 + 3 + 1 commands");
    assert_eq!(mock.commands()[1], vec!["HSET", "h:r1", "n", "1"]);

    // Bytes: each SET here is 34 bytes; 1024-byte pipelines hold 30.
    let mock = Mock::start(false, |_, a| redis_like(a)).await;
    let mut c = config(mock.port, set_cmd());
    c.pipeline_bytes = 1024;
    c.pipeline_rows = 10_000;
    let h = Harness::start(c, owner());
    let rows: Vec<Row> = (0..100).map(|i| row(&format!("{i:04}"), 1, 1.0)).collect();
    h.send(rows).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(resp(&["SET", "dev:0000", "1"]).len(), 34);
    assert_eq!((outbox.acked(), snap.redis_sink_commands_ok), (1, 100));
    assert_eq!(snap.redis_sink_pipelines, 4, "30 + 30 + 30 + 10");
}

#[tokio::test]
async fn error_replies_fail_only_their_batch_and_are_not_retried() {
    let mock = Mock::start(false, |_, a| {
        if a[1] == b"dev:bad" {
            Act::Reply(
                b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
            )
        } else {
            redis_like(a)
        }
    })
    .await;
    let h = Harness::start(config(mock.port, set_cmd()), owner());
    h.send(vec![row("a", 1, 1.0)]).await;
    h.send(vec![row("bad", 2, 1.0), row("c", 3, 1.0)]).await;
    h.send(vec![row("d", 4, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (2, 1));
    assert_eq!(mock.names().len(), 4, "no resend");
    assert_eq!(
        (
            snap.redis_sink_command_errors,
            snap.redis_sink_commands_ok,
            snap.redis_sink_fatal
        ),
        (1, 3, 0)
    );
}

#[tokio::test]
async fn auth_acl_cluster_and_replica_errors_stop_the_sink() {
    for (reply, reason) in [
        (
            &b"-NOAUTH Authentication required.\r\n"[..],
            "redis_auth_or_acl_rejected",
        ),
        (
            b"-NOPERM User app has no permissions to run the 'set' command\r\n",
            "redis_auth_or_acl_rejected",
        ),
        (
            b"-MOVED 3999 127.0.0.1:6381\r\n",
            "redis_cluster_redirect_unsupported",
        ),
        (
            b"-READONLY You can't write against a read only replica.\r\n",
            "redis_readonly_replica",
        ),
    ] {
        let reply = reply.to_vec();
        let mock = Mock::start(false, move |_, _| Act::Reply(reply.clone())).await;
        let h = Harness::start(config(mock.port, set_cmd()), owner());
        let cancel = h.cancel.clone();
        h.send(vec![row("a", 1, 1.0)]).await;
        let (outbox, snap) = h.finish().await;
        assert!(cancel.is_cancelled(), "{reason}");
        assert_eq!(
            (outbox.acked(), outbox.failed(), snap.redis_sink_fatal),
            (0, 1, 1),
            "{reason}"
        );
        assert_eq!(mock.names().len(), 1);
    }
}

#[tokio::test]
async fn tls_auth_with_acl_user_and_db_select() {
    let mock = Mock::start(true, |_, a| redis_like(a)).await;
    let mut c = config(mock.port, set_cmd());
    c.target.url = format!("rediss://localhost:{}/2", mock.port);
    c.target.ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
    c.target.username_secret = Some("user".into());
    c.target.password_secret = Some("pw".into());
    let h = Harness::start(c.clone(), owner());
    h.send(vec![row("a", 1, 1.0)]).await;
    let (outbox, _) = h.finish().await;
    assert_eq!(outbox.acked(), 1);
    assert_eq!(
        mock.commands(),
        vec![
            vec!["AUTH", "app", "hunter2"],
            vec!["SELECT", "2"],
            vec!["SET", "dev:a", "1"],
        ]
    );

    // Wrong password: the handshake is rejected and the job fails.
    c.target.password_secret = Some("bad".into());
    let h = Harness::start(c.clone(), owner());
    let cancel = h.cancel.clone();
    h.send(vec![row("a", 1, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert!(cancel.is_cancelled());
    assert_eq!(
        (
            outbox.failed(),
            snap.redis_sink_fatal,
            snap.redis_sink_retries
        ),
        (1, 1, 0)
    );

    // Default roots do not trust the test CA: retried, then failed, never fatal.
    c.target.password_secret = Some("pw".into());
    c.target.ca_pem = None;
    c.max_retries = 1;
    let h = Harness::start(c, owner());
    h.send(vec![row("a", 1, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(
        (
            outbox.failed(),
            snap.redis_sink_connect_failures,
            snap.redis_sink_fatal
        ),
        (1, 2, 0)
    );
}

#[tokio::test]
async fn lost_connection_resends_idempotent_commands_only() {
    // The first SET on the first connection is "executed" but unanswered.
    let script = |ctx: Ctx, a: &[Vec<u8>]| {
        if ctx.conn == 0 && ctx.seen == 1 {
            Act::Close
        } else {
            redis_like(a)
        }
    };
    let mock = Mock::start(false, script).await;
    let h = Harness::start(config(mock.port, set_cmd()), owner());
    h.send(vec![row("a", 1, 1.0), row("b", 2, 1.0), row("c", 3, 1.0)])
        .await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0));
    // dev:a answered; dev:b and dev:c re-sent on a new connection.
    assert_eq!(
        mock.log
            .lock()
            .unwrap()
            .iter()
            .map(|(c, a)| (*c, String::from_utf8_lossy(&a[1]).to_string()))
            .collect::<Vec<_>>(),
        vec![
            (0, "dev:a".into()),
            (0, "dev:b".into()),
            (1, "dev:b".into()),
            (1, "dev:c".into()),
        ]
    );
    assert_eq!(
        (
            snap.redis_sink_retries,
            snap.redis_sink_unknown_outcome,
            snap.redis_sink_connects
        ),
        (1, 0, 2)
    );

    for command in [
        RedisCommand::Xadd {
            key: tpl("s"),
            fields: vec!["n".into()],
            maxlen: None,
            approximate: false,
        },
        RedisCommand::Publish {
            channel: tpl("c"),
            value: RedisValue::Column("n".into()),
        },
        RedisCommand::Push {
            key: tpl("q"),
            value: RedisValue::Column("n".into()),
            left: false,
        },
    ] {
        let name = command.name();
        let mock = Mock::start(false, script).await;
        let h = Harness::start(config(mock.port, command), owner());
        h.send(vec![row("a", 1, 1.0)]).await;
        h.send(vec![row("b", 2, 1.0), row("c", 3, 1.0)]).await;
        let (outbox, snap) = h.finish().await;
        assert_eq!((outbox.acked(), outbox.failed()), (1, 1), "{name}");
        assert_eq!(mock.names().len(), 2, "{name}: nothing re-sent");
        assert_eq!(
            (snap.redis_sink_unknown_outcome, snap.redis_sink_retries),
            (2, 0),
            "{name}"
        );
    }
}

#[tokio::test]
async fn idle_connection_closed_by_server_is_replaced_before_sending() {
    // Every reply closes the connection afterwards (like a server timeout).
    let mock = Mock::start(false, |_, a| match redis_like(a) {
        Act::Reply(b) => Act::ReplyClose(b),
        other => other,
    })
    .await;
    let mut c = config(
        mock.port,
        RedisCommand::Xadd {
            key: tpl("s"),
            fields: vec!["n".into()],
            maxlen: None,
            approximate: false,
        },
    );
    c.pipeline_rows = 1;
    let h = Harness::start(c, owner());
    h.send(vec![row("a", 1, 1.0)]).await;
    h.settled(1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.send(vec![row("b", 2, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (2, 0));
    assert_eq!(mock.names().len(), 2, "each XADD sent exactly once");
    assert_eq!(
        (snap.redis_sink_connects, snap.redis_sink_unknown_outcome),
        (2, 0)
    );
}

#[tokio::test]
async fn timeouts_and_refused_connections_retry_with_backoff_then_fail() {
    // An unanswered SET times out and is re-sent on a new connection.
    let mock = Mock::start(false, |ctx, a| {
        if ctx.conn == 0 {
            Act::Hang
        } else {
            redis_like(a)
        }
    })
    .await;
    let mut c = config(mock.port, set_cmd());
    c.timeout = Duration::from_millis(200);
    let h = Harness::start(c, owner());
    h.send(vec![row("a", 1, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), snap.redis_sink_retries), (1, 1));

    // Nothing listening: every attempt fails to connect (nothing was sent,
    // so even XADD may be retried), then the batch fails.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let mut c = config(
        port,
        RedisCommand::Xadd {
            key: tpl("s"),
            fields: vec!["n".into()],
            maxlen: None,
            approximate: false,
        },
    );
    c.max_retries = 2;
    let h = Harness::start(c, owner());
    h.send(vec![row("a", 1, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(outbox.failed(), 1);
    assert_eq!(
        (snap.redis_sink_connect_failures, snap.redis_sink_retries),
        (3, 2)
    );
    assert_eq!(snap.redis_sink_fatal, 0);
}

#[tokio::test]
async fn stop_deadline_covers_the_pipeline_in_flight() {
    let mock = Mock::start(false, |_, _| Act::Hang).await;
    let mut c = config(mock.port, set_cmd());
    c.timeout = Duration::from_secs(60);
    c.flush_timeout = Duration::from_millis(200);
    let h = Harness::start(c, owner());
    h.send(vec![row("a", 1, 1.0), row("b", 1, 1.0)]).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while mock.names().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let started = std::time::Instant::now();
    h.cancel.cancel();
    let owner = h.owner.clone();
    let (outbox, snap) = h.finish().await;
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(190) && took < Duration::from_secs(2),
        "{took:?}"
    );
    assert_eq!(outbox.failed(), 1);
    assert_eq!(snap.redis_sink_discarded_on_close, 2);
    assert_eq!(
        owner.usage().reservation_bytes,
        0,
        "pipeline and connection released"
    );
}

#[tokio::test]
async fn bad_oversize_and_unbudgeted_rows_are_dropped_and_counted() {
    let mock = Mock::start(false, |_, a| redis_like(a)).await;
    let mut c = config(mock.port, set_cmd());
    c.pipeline_bytes = 1024;
    let h = Harness::start(c, owner());
    let mut null_key = row("x", 1, 1.0);
    null_key.values[0] = Scalar::Null;
    h.send(vec![row("a", 1, 1.0), null_key]).await;
    h.send(vec![row(&"k".repeat(2000), 1, 1.0)]).await;
    h.send(vec![row("b", 1, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (1, 2));
    assert_eq!(
        (
            snap.redis_sink_dropped_bad,
            snap.redis_sink_dropped_oversize
        ),
        (1, 1)
    );
    assert_eq!(
        mock.commands(),
        vec![vec!["SET", "dev:a", "1"], vec!["SET", "dev:b", "1"]]
    );

    // Rendered key over 4096 bytes is a bad row, not an oversize one.
    let mock = Mock::start(false, |_, a| redis_like(a)).await;
    let h = Harness::start(config(mock.port, set_cmd()), owner());
    h.send(vec![row(&"k".repeat(5000), 1, 1.0)]).await;
    let (_, snap) = h.finish().await;
    assert_eq!(snap.redis_sink_dropped_bad, 1);
}

#[tokio::test]
async fn input_schema_that_does_not_fit_the_command_stops_the_sink() {
    let mock = Mock::start(false, |_, a| redis_like(a)).await;
    let h = Harness::start(
        config(
            mock.port,
            RedisCommand::Set {
                key: tpl("{missing}"),
                value: RedisValue::Column("n".into()),
                ttl: None,
            },
        ),
        owner(),
    );
    let cancel = h.cancel.clone();
    h.send(vec![row("a", 1, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert!(cancel.is_cancelled());
    assert_eq!((outbox.failed(), snap.redis_sink_fatal), (1, 1));
    assert!(mock.names().is_empty());
}

#[test]
fn config_bounds_commands_and_reservation_math() {
    let c = config(1, set_cmd());
    c.validate().unwrap();
    let bad: Vec<Mutation<RedisSinkConfig>> = vec![
        Box::new(|c| c.pipeline_rows = 0),
        Box::new(|c| c.pipeline_rows = 10_001),
        Box::new(|c| c.pipeline_bytes = 1023),
        Box::new(|c| c.pipeline_bytes = 4 * 1024 * 1024 + 1),
        Box::new(|c| c.timeout = Duration::from_millis(99)),
        Box::new(|c| c.max_retries = 21),
        Box::new(|c| c.retry_max = Duration::from_millis(5)),
        Box::new(|c| c.flush_timeout = Duration::from_secs(61)),
        Box::new(|c| c.outbox_capacity = 0),
        Box::new(|c| {
            c.restore = sparrow_model::RestoreClaim::Checkpoint {
                snapshot_id: "s".into(),
            }
        }),
        Box::new(|c| {
            c.command = RedisCommand::Set {
                key: tpl("k"),
                value: RedisValue::Json,
                ttl: Some(Duration::ZERO),
            }
        }),
        Box::new(|c| {
            c.command = RedisCommand::Set {
                key: tpl("k"),
                value: RedisValue::Json,
                ttl: Some(Duration::from_millis(sink::MAX_TTL_MS + 1)),
            }
        }),
        Box::new(|c| {
            c.command = RedisCommand::Xadd {
                key: tpl("k"),
                fields: vec!["n".into()],
                maxlen: Some(0),
                approximate: false,
            }
        }),
        Box::new(|c| {
            c.command = RedisCommand::Hset {
                key: tpl("k"),
                fields: HashFields::Columns(vec!["n".into(), "n".into()]),
            }
        }),
        Box::new(|c| {
            c.command = RedisCommand::Xadd {
                key: tpl("k"),
                fields: vec![],
                maxlen: None,
                approximate: false,
            }
        }),
    ];
    for (i, f) in bad.iter().enumerate() {
        let mut c = config(1, set_cmd());
        f(&mut c);
        assert!(c.validate().is_err(), "case {i}");
    }
    // Reservation: checked arithmetic, half of the job reservation.
    let mut c = config(1, set_cmd());
    c.pipeline_bytes = usize::MAX;
    assert_eq!(c.peak_bytes(), None);
    assert_eq!(
        c.check_reservation_budget(usize::MAX).unwrap_err().code,
        ErrorCode::BoundExceeded
    );
    let mut c = config(1, set_cmd());
    c.pipeline_rows = usize::MAX;
    assert_eq!(c.peak_bytes(), None);
    let c = config(1, set_cmd());
    let peak = c.peak_bytes().unwrap();
    c.check_reservation_budget(peak * 2).unwrap();
    assert!(c.check_reservation_budget(peak * 2 - 2).is_err());
    let mut json = config(1, set_cmd());
    json.command = RedisCommand::Publish {
        channel: tpl("c"),
        value: RedisValue::Json,
    };
    assert_eq!(json.peak_bytes().unwrap(), peak + json.pipeline_bytes);
    // Compile-time column checks.
    let s = schema();
    assert!(set_cmd().compile(&s).is_ok());
    for command in [
        RedisCommand::Set {
            key: tpl("{v}"),
            value: RedisValue::Json,
            ttl: None,
        },
        RedisCommand::Set {
            key: tpl("k"),
            value: RedisValue::Column("nope".into()),
            ttl: None,
        },
        RedisCommand::Hset {
            key: tpl("k"),
            fields: HashFields::Columns(vec!["nope".into()]),
        },
    ] {
        assert!(command.compile(&s).is_err(), "{command:?}");
    }
    assert!(RedisCommand::Set {
        key: tpl("k"),
        value: RedisValue::Json,
        ttl: None
    }
    .idempotent());
    assert!(!RedisCommand::Publish {
        channel: tpl("k"),
        value: RedisValue::Json
    }
    .idempotent());
}

#[test]
fn bind_refuses_policy_budget_and_secret_problems_and_debug_is_redacted() {
    let diag = IoDiagnostics::new();
    let policy = TargetPolicy::allow("localhost", 6380);
    let mut c = config(6380, set_cmd());
    c.target.url = "rediss://localhost:6380".into();
    c.target.password_secret = Some("pw".into());
    let sink = RedisSink::bind(c.clone(), &secrets(), &policy, owner(), diag.clone()).unwrap();
    let shown = format!("{sink:?}");
    assert!(
        !shown.contains("hunter2") && !shown.contains("localhost"),
        "{shown}"
    );
    let err = RedisSink::bind(
        c.clone(),
        &secrets(),
        &TargetPolicy::deny_all(),
        owner(),
        diag.clone(),
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);
    let mut missing = c.clone();
    missing.target.password_secret = Some("absent".into());
    assert!(RedisSink::bind(missing, &secrets(), &policy, owner(), diag.clone()).is_err());
    let mut tiny = ResourceBudget::compact();
    tiny.reservation_bytes = 256 * 1024;
    let err = RedisSink::bind(c, &secrets(), &policy, MemoryOwner::new(tiny), diag).unwrap_err();
    assert_eq!(err.code, ErrorCode::BoundExceeded);
}

#[test]
fn backoff_is_capped_jittered_and_saturating() {
    let (i, m) = (Duration::from_millis(100), Duration::from_secs(5));
    assert_eq!(sink::backoff(i, m, 1, 0), Duration::from_millis(50));
    assert_eq!(
        sink::backoff(i, m, 1, u64::MAX),
        Duration::from_millis(100).min(sink::backoff(i, m, 1, u64::MAX))
    );
    for attempt in [1, 2, 10, u32::MAX] {
        for r in [0, 7, u64::MAX] {
            let d = sink::backoff(i, m, attempt, r);
            assert!(
                d <= m && d >= Duration::from_millis(50),
                "{attempt} {r} {d:?}"
            );
        }
    }
    assert_eq!(sink::backoff(i, m, 40, 0), m / 2);
}

// -------------------------------------------------------- lookup tests --

pub(super) fn lookup_schema() -> Schema {
    Schema::new(
        SchemaId::new(7),
        vec![
            Field::new(FieldId::new(1), "site", DataType::Utf8, false),
            Field::new(FieldId::new(2), "dev", DataType::Int64, false),
            Field::new(FieldId::new(3), "limit", DataType::Float64, false),
            Field::new(FieldId::new(4), "label", DataType::Utf8, true),
            Field::new(FieldId::new(5), "on", DataType::Bool, true),
        ],
    )
    .unwrap()
}

pub(super) fn lookup_config(url: String, format: RedisLookupFormat) -> RedisLookupConfig {
    RedisLookupConfig {
        target: RedisTarget::new(url),
        schema: lookup_schema(),
        keys: vec!["site".into(), "dev".into()],
        key: "lim:{site}:{dev}".into(),
        format,
        timeout: Duration::from_millis(1000),
        pool_size: 2,
    }
}

pub(super) fn key(site: &str, dev: i64) -> Vec<Scalar> {
    vec![Scalar::utf8(site), Scalar::Int64(dev)]
}

fn store_script(
    store: HashMap<&'static str, Act>,
) -> impl Fn(Ctx, &[Vec<u8>]) -> Act + Send + Sync + 'static {
    move |_, a| match a[0].as_slice() {
        b"HMGET" | b"GET" => store
            .get(std::str::from_utf8(&a[1]).unwrap())
            .cloned()
            .unwrap_or(if a[0] == b"GET" {
                Act::Reply(b"$-1\r\n".to_vec())
            } else {
                Act::Reply(b"*3\r\n$-1\r\n$-1\r\n$-1\r\n".to_vec())
            }),
        _ => redis_like(a),
    }
}

fn hash3(a: &str, b: Option<&str>, c: Option<&str>) -> Act {
    let mut v = b"*3\r\n".to_vec();
    for f in [Some(a), b, c] {
        match f {
            Some(s) => v.extend_from_slice(format!("${}\r\n{s}\r\n", s.len()).as_bytes()),
            None => v.extend_from_slice(b"$-1\r\n"),
        }
    }
    Act::Reply(v)
}

fn bind_lookup(config: RedisLookupConfig) -> RedisLookup {
    let ep = config.target.endpoint().unwrap();
    RedisLookup::bind(config, &secrets(), &TargetPolicy::allow(ep.host, ep.port)).unwrap()
}

#[tokio::test]
async fn hash_lookup_bounds_the_sum_of_fields_before_decoding_them() {
    for (width, accepted) in [(16 * 1024, true), (40 * 1024, false)] {
        let value = "x".repeat(width);
        let reply = hash3(&value, Some(&value), Some(&value));
        let mock = Mock::start(false, move |_, _| reply.clone()).await;
        let mut cfg = lookup_config(
            format!("redis://127.0.0.1:{}", mock.port),
            RedisLookupFormat::Hash,
        );
        for field in &mut cfg.schema.fields[2..] {
            field.data_type = DataType::Utf8;
        }
        let lookup = bind_lookup(cfg);
        let result = lookup.lookup(key("a", 1), CancellationToken::new()).await;
        if accepted {
            assert!(
                result.unwrap().unwrap().resident_bytes() <= lookup::REDIS_LOOKUP_MAX_ROW_BYTES
            );
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::BoundExceeded);
        }
    }
}

#[tokio::test]
async fn relative_ttl_set_is_not_replayed_after_an_unknown_outcome() {
    let mock = Mock::start(false, |_, _| Act::Close).await;
    let mut command = set_cmd();
    if let RedisCommand::Set { ttl, .. } = &mut command {
        *ttl = Some(Duration::from_secs(1));
    }
    assert!(!command.idempotent());
    assert!(set_cmd().idempotent());
    let h = Harness::start(config(mock.port, command), owner());
    h.send(vec![row("ttl", 1, 1.0)]).await;
    let (outbox, diag) = h.finish().await;
    assert_eq!(outbox.failed(), 1);
    assert_eq!(diag.redis_sink_unknown_outcome, 1);
    assert_eq!(diag.redis_sink_retries, 0);
    assert_eq!(mock.names().len(), 1, "resending PX would restart the TTL");
}

#[tokio::test]
async fn idle_probe_rejects_partial_unsolicited_replies() {
    let (io, mut peer) = tokio::io::duplex(64);
    peer.write_all(b"+O").await.unwrap();
    let mut conn = conn::Connection { io: Box::pin(io) };
    let mut reader = resp::Reader::new(64, 1);
    assert!(!conn.probe_alive(&mut reader).await);
    assert!(reader.has_buffered());
}

#[tokio::test]
async fn resp_line_limit_is_inclusive_and_size_math_saturates() {
    for size in [resp::MAX_LINE, resp::MAX_LINE + 1] {
        let wire = format!("+{}\r\n", "x".repeat(size));
        let mut input = wire.as_bytes();
        let mut reader = resp::Reader::new(1, 1);
        let result = reader.next(&mut input).await;
        if size == resp::MAX_LINE {
            let resp::Frame::Simple(range) = result.unwrap() else {
                panic!("simple reply")
            };
            assert_eq!(reader.bytes(range).len(), size);
        } else {
            assert!(matches!(result, Err(resp::ReadError::Protocol(_))));
        }
    }
    assert_eq!(resp::bulk_len(usize::MAX), usize::MAX);
}

#[tokio::test]
async fn hash_lookup_pipelines_batches_and_types_values() {
    let mut store = HashMap::new();
    store.insert("lim:a:1", hash3("2.5", Some("pump"), Some("1")));
    store.insert("lim:a:2", hash3("-1e3", None, None));
    let mock = Mock::start(false, store_script(store)).await;
    let lookup = bind_lookup(lookup_config(
        format!("redis://127.0.0.1:{}", mock.port),
        RedisLookupFormat::Hash,
    ));
    let rows = lookup
        .lookup_batch(
            vec![key("a", 1), key("a", 2), key("b", 9)],
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            Some(Row {
                values: vec![
                    Scalar::utf8("a"),
                    Scalar::Int64(1),
                    Scalar::Float64(2.5),
                    Scalar::utf8("pump"),
                    Scalar::Bool(true)
                ]
            }),
            Some(Row {
                values: vec![
                    Scalar::utf8("a"),
                    Scalar::Int64(2),
                    Scalar::Float64(-1000.0),
                    Scalar::Null,
                    Scalar::Null
                ]
            }),
            None,
        ]
    );
    // One write carrying three HMGETs, exact bytes.
    let expected: Vec<u8> = [("a", 1), ("a", 2), ("b", 9)]
        .iter()
        .flat_map(|(s, d)| resp(&["HMGET", &format!("lim:{s}:{d}"), "limit", "label", "on"]))
        .collect();
    assert_eq!(mock.raw_all(), expected);
    // Pool reuse: one connection for a second request.
    lookup
        .lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mock.conns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn json_lookup_decodes_flat_objects_strictly() {
    let mut store = HashMap::new();
    store.insert(
        "lim:a:1",
        bulk(br#"{"site":"a","dev":1,"limit":3.0,"label":"x","extra":true}"#),
    );
    store.insert("lim:a:2", bulk(br#"{"site":"a","dev":2,"limit":1}"#));
    store.insert(
        "lim:n:1",
        bulk(br#"{"site":"n","dev":1,"limit":1,"o":{"x":1}}"#),
    );
    store.insert("lim:r:1", bulk(br#"{"site":"r","dev":1}"#));
    let mock = Mock::start(false, store_script(store)).await;
    let lookup = bind_lookup(lookup_config(
        format!("redis://127.0.0.1:{}", mock.port),
        RedisLookupFormat::Json,
    ));
    let rows = lookup
        .lookup_batch(
            vec![key("a", 1), key("a", 2), key("z", 1)],
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows[0].as_ref().unwrap().values[3], Scalar::utf8("x"));
    assert_eq!(rows[1].as_ref().unwrap().values[2], Scalar::Float64(1.0));
    assert_eq!(rows[1].as_ref().unwrap().values[3], Scalar::Null);
    assert_eq!(rows[2], None);
    assert_eq!(
        mock.raw_all(),
        [
            resp(&["GET", "lim:a:1"]),
            resp(&["GET", "lim:a:2"]),
            resp(&["GET", "lim:z:1"])
        ]
        .concat()
    );
    for (k, code) in [
        (key("n", 1), ErrorCode::CodecViolation),
        (key("r", 1), ErrorCode::CodecViolation),
    ] {
        let e = lookup
            .lookup(k, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(e.code, code, "{}", e.message);
    }
    let many = format!(
        "{{{}}}",
        (0..65)
            .map(|i| format!("\"m{i}\":1"))
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(decode_json_shape_rejects(&many));
}

fn decode_json_shape_rejects(body: &str) -> bool {
    lookup::decode_json(&lookup_schema(), body.as_bytes()).is_err()
}

#[tokio::test]
async fn lookup_errors_are_classified_hard_or_transport() {
    let mut store = HashMap::new();
    store.insert(
        "lim:w:1",
        Act::Reply(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        ),
    );
    store.insert(
        "lim:l:1",
        Act::Reply(b"-LOADING Redis is loading the dataset in memory\r\n".to_vec()),
    );
    store.insert("lim:m:1", Act::Reply(b"-MOVED 1 127.0.0.1:1\r\n".to_vec()));
    store.insert(
        "lim:p:1",
        Act::Reply(b"-NOPERM no permissions\r\n".to_vec()),
    );
    store.insert("lim:t:1", hash3("not-a-number", None, None));
    store.insert("lim:b:1", hash3("1", None, Some("yes")));
    store.insert("lim:q:1", hash3("1", None, None));
    store.insert(
        "lim:r:1",
        Act::Reply(b"*3\r\n$-1\r\n$1\r\nx\r\n$-1\r\n".to_vec()),
    );
    store.insert("lim:s:1", Act::Reply(b"*2\r\n$-1\r\n$-1\r\n".to_vec()));
    store.insert(
        "lim:o:1",
        Act::Reply(format!("$70000\r\n{}\r\n", "x".repeat(70000)).into_bytes()),
    );
    store.insert("lim:h:1", Act::Hang);
    let mock = Mock::start(false, store_script(store)).await;
    let mut config = lookup_config(
        format!("redis://127.0.0.1:{}", mock.port),
        RedisLookupFormat::Hash,
    );
    config.timeout = Duration::from_millis(200);
    let lookup = bind_lookup(config);
    for (site, code) in [
        ("w", ErrorCode::TypeMismatch),
        ("l", ErrorCode::JobFailed),
        ("m", ErrorCode::FeatureUnavailable),
        ("p", ErrorCode::PolicyDenied),
        ("t", ErrorCode::CodecViolation),
        ("b", ErrorCode::CodecViolation),
        ("r", ErrorCode::CodecViolation),
        ("s", ErrorCode::CodecViolation),
        ("o", ErrorCode::CodecViolation),
        ("h", ErrorCode::JobFailed),
    ] {
        let e = lookup
            .lookup(key(site, 1), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(e.code, code, "{site}: {}", e.message);
    }
    // A required field absent while another is present.
    let ok = lookup
        .lookup(key("q", 1), CancellationToken::new())
        .await
        .unwrap();
    assert!(ok.is_some());
    // Keys that would make the rendered key ambiguous are refused.
    let e = lookup
        .lookup(key("a:b", 1), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn reused_connection_closed_by_server_is_retried_once_on_a_fresh_one() {
    let mut store = HashMap::new();
    store.insert("lim:a:1", hash3("1", None, None));
    let inner = store_script(store);
    // Connection 0 answers once then "drops" the next request; others fine.
    let mock = Mock::start(false, move |ctx, a| {
        if ctx.conn == 0 && ctx.seen == 1 {
            Act::Close
        } else {
            inner(ctx, a)
        }
    })
    .await;
    let mut config = lookup_config(
        format!("redis://127.0.0.1:{}", mock.port),
        RedisLookupFormat::Hash,
    );
    config.pool_size = 1;
    let lookup = bind_lookup(config);
    for _ in 0..3 {
        assert!(lookup
            .lookup(key("a", 1), CancellationToken::new())
            .await
            .unwrap()
            .is_some());
    }
    assert_eq!(mock.conns.load(Ordering::SeqCst), 2);
    assert_eq!(mock.names().len(), 4, "one repeat of the dropped read");

    // A fresh connection that dies is a transport error (no second retry).
    let mock = Mock::start(false, |_, _| Act::Close).await;
    let lookup = bind_lookup(lookup_config(
        format!("redis://127.0.0.1:{}", mock.port),
        RedisLookupFormat::Hash,
    ));
    let e = lookup
        .lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::JobFailed);
    assert_eq!(mock.names().len(), 1);
}

#[tokio::test]
async fn lookup_pool_bounds_connections_and_cancel_stops_io() {
    let mock = Mock::start(false, |_, _| Act::Hang).await;
    let mut config = lookup_config(
        format!("redis://127.0.0.1:{}", mock.port),
        RedisLookupFormat::Hash,
    );
    config.pool_size = 2;
    config.timeout = Duration::from_secs(5);
    let lookup = Arc::new(bind_lookup(config));
    let cancel = CancellationToken::new();
    let tasks: Vec<_> = (0..5)
        .map(|i| {
            let (lookup, cancel) = (lookup.clone(), cancel.clone());
            tokio::spawn(async move { lookup.lookup(key("a", i), cancel).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        mock.conns.load(Ordering::SeqCst),
        2,
        "pool_size bounds connections"
    );
    let started = std::time::Instant::now();
    cancel.cancel();
    for t in tasks {
        assert_eq!(t.await.unwrap().unwrap_err().code, ErrorCode::Cancelled);
    }
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn lookup_tls_auth_and_config_checks() {
    let mut store = HashMap::new();
    store.insert("lim:a:1", hash3("1", None, None));
    let inner = store_script(store);
    let mock = Mock::start(true, inner).await;
    let mut config = lookup_config(
        format!("rediss://localhost:{}", mock.port),
        RedisLookupFormat::Hash,
    );
    config.target.ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
    config.target.password_secret = Some("pw".into());
    let lookup = bind_lookup(config.clone());
    assert!(lookup
        .lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap()
        .is_some());
    assert_eq!(mock.names(), vec!["AUTH", "HMGET"]);
    config.target.password_secret = Some("bad".into());
    let e = bind_lookup(config.clone())
        .lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::PolicyDenied);

    let base = lookup_config("redis://h".into(), RedisLookupFormat::Hash);
    RedisLookup::check(&base).unwrap();
    let cases: Vec<Mutation<RedisLookupConfig>> = vec![
        Box::new(|c| c.key = "lim:{site}".into()),
        Box::new(|c| c.key = "lim:{site}:{dev}:{label}".into()),
        Box::new(|c| c.key = "{site}{dev}".into()),
        Box::new(|c| c.keys = vec!["limit".into()]),
        Box::new(|c| c.keys = vec!["label".into()]),
        Box::new(|c| c.keys = vec![]),
        Box::new(|c| c.pool_size = 0),
        Box::new(|c| c.pool_size = 17),
        Box::new(|c| c.timeout = Duration::from_millis(9)),
        Box::new(|c| c.timeout = Duration::from_millis(5001)),
        Box::new(|c| {
            c.schema = Schema::new(
                SchemaId::new(1),
                vec![
                    Field::new(FieldId::new(1), "site", DataType::Utf8, false),
                    Field::new(FieldId::new(2), "dev", DataType::Int64, false),
                ],
            )
            .unwrap()
        }),
        Box::new(|c| c.target.password_secret = Some("pw".into())),
    ];
    for (i, f) in cases.iter().enumerate() {
        let mut c = base.clone();
        f(&mut c);
        assert!(RedisLookup::check(&c).is_err(), "case {i}");
    }
    // Key-only schemas are fine for JSON values.
    let mut c = base.clone();
    c.format = RedisLookupFormat::Json;
    c.schema = Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "site", DataType::Utf8, false),
            Field::new(FieldId::new(2), "dev", DataType::Int64, false),
        ],
    )
    .unwrap();
    RedisLookup::check(&c).unwrap();
}

#[test]
fn text_values_parse_strictly() {
    use lookup::parse_text;
    assert_eq!(parse_text(b"-5", &DataType::Int64), Some(Scalar::Int64(-5)));
    assert_eq!(
        parse_text(b"18446744073709551615", &DataType::UInt64),
        Some(Scalar::UInt64(u64::MAX))
    );
    assert_eq!(
        parse_text(b"1700000000000000", &DataType::TimestampMicrosUTC),
        Some(Scalar::TimestampMicrosUTC(1_700_000_000_000_000))
    );
    assert_eq!(parse_text(b"0", &DataType::Bool), Some(Scalar::Bool(false)));
    assert_eq!(
        parse_text(b"\xff", &DataType::Bytes),
        Some(Scalar::Bytes(b"\xff".to_vec().into()))
    );
    for (bad, t) in [
        (&b"+5"[..], DataType::Int64),
        (b" 5", DataType::Int64),
        (b"9223372036854775808", DataType::Int64),
        (b"-1", DataType::UInt64),
        (b"", DataType::UInt64),
        (b"nan", DataType::Float64),
        (b"inf", DataType::Float64),
        (b"1e999", DataType::Float64),
        (b"yes", DataType::Bool),
        (b"\xff", DataType::Utf8),
    ] {
        assert_eq!(parse_text(bad, &t), None, "{bad:?} {t:?}");
    }
}

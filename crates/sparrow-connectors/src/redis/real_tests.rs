//! Opt-in tests against a real `redis-server` (Redis 6.2 / 7.x, RESP2).
//!
//! Set `SPARROW_REDIS_SERVER` to the path of a `redis-server` binary built
//! with TLS (`BUILD_TLS=yes`); every test is a no-op otherwise. Each test
//! starts its own server on free ports with a plain and a TLS listener and
//! these ACL users: `default` (no password), `app` (all commands) and
//! `reader` (read commands only), both with password `hunter2`.

use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sparrow_model::{
    CreditKind, DataType, ErrorCode, Field, FieldId, MemoryOwner, ResourceBudget, Result, Row,
    RowBatch, RowBatchBuilder, Scalar, Schema, SchemaId,
};
use sparrow_plan::LookupSpec;
use sparrow_runtime::external_lookup::ExternalLookupOperator;
use sparrow_runtime::{
    ExternalLookup, ExternalLookupBinding, ExternalLookupOptions, LookupErrorPolicy,
};
use tokio_util::sync::CancellationToken;

use super::tests::{key, lookup_config, owner, row, set_cmd, tpl, Harness};
use super::tls_fixture;
use super::*;
use crate::{MapSecretResolver, TargetPolicy};

// ------------------------------------------------------------- server --

struct Server {
    child: Child,
    dir: PathBuf,
    port: u16,
    tls_port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.dir.as_os_str().is_empty() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl Server {
    fn start() -> Option<Self> {
        let Some(bin) = std::env::var_os("SPARROW_REDIS_SERVER") else {
            eprintln!("SPARROW_REDIS_SERVER not set; skipping real Redis test");
            return None;
        };
        let dir = std::env::temp_dir().join(format!(
            "sparrow-redis-{}-{}",
            std::process::id(),
            free_port()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        for (name, pem) in [
            ("ca.pem", tls_fixture::CA),
            ("cert.pem", tls_fixture::CERT),
            ("key.pem", tls_fixture::KEY),
        ] {
            std::fs::write(dir.join(name), pem).unwrap();
        }
        // A freshly probed port can be taken by another socket before the
        // server binds it; retry with new ports if the server exits.
        for _ in 0..5 {
            let (port, tls_port) = (free_port(), free_port());
            let conf = format!(
                "bind 127.0.0.1\nport {port}\ntls-port {tls_port}\n\
                 tls-cert-file {d}/cert.pem\ntls-key-file {d}/key.pem\ntls-ca-cert-file {d}/ca.pem\n\
                 tls-auth-clients no\nsave \"\"\nappendonly no\ndir {d}\n\
                 user default on nopass ~* &* +@all\n\
                 user app on >hunter2 ~* &* +@all\n\
                 user reader on >hunter2 ~* &* -@all +@read +ping\n",
                d = dir.display()
            );
            std::fs::write(dir.join("redis.conf"), conf).unwrap();
            let child = Command::new(&bin)
                .arg(dir.join("redis.conf"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn redis-server");
            let mut server = Self {
                child,
                dir: dir.clone(),
                port,
                tls_port,
            };
            let started = Instant::now();
            loop {
                if server.try_cmd(&["PING"]).is_ok() {
                    return Some(server);
                }
                if server.child.try_wait().unwrap().is_some() {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(10),
                    "redis-server did not start"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            // Keep the directory for the next attempt.
            server.dir = PathBuf::new();
        }
        panic!("redis-server did not start");
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }

    fn tls_url(&self) -> String {
        format!("rediss://localhost:{}", self.tls_port)
    }

    fn try_cmd(&self, args: &[&str]) -> std::io::Result<V> {
        let mut s = TcpStream::connect(("127.0.0.1", self.port))?;
        s.set_read_timeout(Some(Duration::from_secs(10)))?;
        let mut out = Vec::new();
        resp::push_command(
            &mut out,
            &args.iter().map(|a| a.as_bytes()).collect::<Vec<_>>(),
        );
        s.write_all(&out)?;
        read_value(&mut BufReader::new(s))
    }

    /// Independent raw client (blocking, own parser) for verification.
    fn cmd(&self, args: &[&str]) -> V {
        self.try_cmd(args).unwrap()
    }

    fn version(&self) -> String {
        let V::Bulk(Some(info)) = self.cmd(&["INFO", "server"]) else {
            panic!()
        };
        String::from_utf8(info)
            .unwrap()
            .lines()
            .find_map(|l| l.strip_prefix("redis_version:").map(str::to_string))
            .unwrap()
    }
}

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<V>>),
}

impl V {
    fn text(&self) -> String {
        match self {
            V::Bulk(Some(b)) => String::from_utf8(b.clone()).unwrap(),
            V::Simple(s) => s.clone(),
            other => panic!("not text: {other:?}"),
        }
    }
    fn texts(&self) -> Vec<String> {
        match self {
            V::Array(Some(v)) => v.iter().map(V::text).collect(),
            other => panic!("not an array: {other:?}"),
        }
    }
    fn int(&self) -> i64 {
        match self {
            V::Int(n) => *n,
            other => panic!("not an integer: {other:?}"),
        }
    }
}

fn read_value(r: &mut impl BufRead) -> std::io::Result<V> {
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let body = line.trim_end_matches("\r\n");
    let (tag, rest) = body.split_at(1);
    Ok(match tag {
        "+" => V::Simple(rest.into()),
        "-" => V::Error(rest.into()),
        ":" => V::Int(rest.parse().unwrap()),
        "$" => {
            let n: i64 = rest.parse().unwrap();
            if n < 0 {
                V::Bulk(None)
            } else {
                let mut buf = vec![0; n as usize + 2];
                std::io::Read::read_exact(r, &mut buf)?;
                buf.truncate(n as usize);
                V::Bulk(Some(buf))
            }
        }
        "*" => {
            let n: i64 = rest.parse().unwrap();
            if n < 0 {
                V::Array(None)
            } else {
                let mut items = Vec::new();
                for _ in 0..n {
                    items.push(read_value(r)?);
                }
                V::Array(Some(items))
            }
        }
        other => panic!("unexpected RESP type {other}"),
    })
}

fn secrets(user: &str) -> MapSecretResolver {
    MapSecretResolver::new(
        [("pw", "hunter2"), ("user", user), ("bad", "nope")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

fn sink_config(url: String, command: RedisCommand) -> RedisSinkConfig {
    let mut c = super::tests::config(0, command);
    c.target.url = url;
    c
}

fn tls(target: &mut RedisTarget, user: Option<&str>) {
    target.ca_pem = Some(String::from_utf8(tls_fixture::CA.to_vec()).unwrap());
    target.password_secret = Some("pw".into());
    target.username_secret = user.map(|_| "user".into());
}

fn rows(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| row(&format!("d{i}"), i as i64, i as f64 / 2.0))
        .collect()
}

// --------------------------------------------------------- sink tests --

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_commands_write_what_they_claim() {
    let Some(server) = Server::start() else {
        return;
    };
    eprintln!("redis {}", server.version());

    // SET with TTL.
    let mut cmd = set_cmd();
    if let RedisCommand::Set { ttl, .. } = &mut cmd {
        *ttl = Some(Duration::from_secs(60));
    }
    let h = Harness::start(sink_config(server.url(), cmd), owner());
    h.send(rows(3)).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), snap.redis_sink_commands_ok), (1, 3));
    assert_eq!(server.cmd(&["GET", "dev:d2"]).text(), "2");
    let pttl = server.cmd(&["PTTL", "dev:d2"]).int();
    assert!((50_000..=60_000).contains(&pttl), "{pttl}");

    // HSET of columns; NULL fields are skipped.
    let h = Harness::start(
        sink_config(
            server.url(),
            RedisCommand::Hset {
                key: tpl("h:{id}"),
                fields: HashFields::Columns(vec!["n".into(), "v".into(), "ok".into()]),
            },
        ),
        owner(),
    );
    let mut r = rows(2);
    r[1].values[2] = Scalar::Null;
    h.send(r).await;
    h.finish().await;
    assert_eq!(
        server.cmd(&["HGETALL", "h:d0"]).texts(),
        ["n", "0", "v", "0.0", "ok", "true"]
    );
    assert_eq!(
        server.cmd(&["HGETALL", "h:d1"]).texts(),
        ["n", "1", "ok", "true"]
    );

    // XADD with exact MAXLEN trims the stream.
    let h = Harness::start(
        sink_config(
            server.url(),
            RedisCommand::Xadd {
                key: tpl("s"),
                fields: vec!["id".into(), "n".into()],
                maxlen: Some(5),
                approximate: false,
            },
        ),
        owner(),
    );
    h.send(rows(8)).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), snap.redis_sink_commands_ok), (1, 8));
    assert_eq!(server.cmd(&["XLEN", "s"]).int(), 5);
    let V::Array(Some(last)) = server.cmd(&["XREVRANGE", "s", "+", "-", "COUNT", "1"]) else {
        panic!()
    };
    let V::Array(Some(entry)) = &last[0] else {
        panic!()
    };
    assert_eq!(entry[1].texts(), ["id", "d7", "n", "7"]);

    // LPUSH / RPUSH order.
    for (left, expect) in [(true, ["d2", "d1", "d0"]), (false, ["d0", "d1", "d2"])] {
        let k = format!("q{left}");
        let h = Harness::start(
            sink_config(
                server.url(),
                RedisCommand::Push {
                    key: tpl(&k),
                    value: RedisValue::Column("id".into()),
                    left,
                },
            ),
            owner(),
        );
        h.send(rows(3)).await;
        h.finish().await;
        assert_eq!(server.cmd(&["LRANGE", &k, "0", "-1"]).texts(), expect);
    }

    // PUBLISH JSON reaches a subscriber; with none, it still acks and counts.
    let port = server.port;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let sub = std::thread::spawn(move || {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\nch.d0\r\n")
            .unwrap();
        let mut r = BufReader::new(s);
        read_value(&mut r).unwrap();
        ready_tx.send(()).unwrap();
        read_value(&mut r).unwrap()
    });
    ready_rx.recv().unwrap();
    let h = Harness::start(
        sink_config(
            server.url(),
            RedisCommand::Publish {
                channel: tpl("ch.{id}"),
                value: RedisValue::Json,
            },
        ),
        owner(),
    );
    h.send(rows(2)).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!(
        (outbox.acked(), snap.redis_sink_publish_no_receivers),
        (1, 1)
    );
    let msg = sub.join().unwrap().texts();
    assert_eq!(msg[0], "message");
    assert_eq!(msg[2], r#"{"id":"d0","n":0,"ok":true,"v":0.0}"#);
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_wrongtype_fails_only_its_batch() {
    let Some(server) = Server::start() else {
        return;
    };
    server.cmd(&["LPUSH", "h:d1", "x"]);
    let h = Harness::start(
        sink_config(
            server.url(),
            RedisCommand::Hset {
                key: tpl("h:{id}"),
                fields: HashFields::Columns(vec!["n".into()]),
            },
        ),
        owner(),
    );
    h.send(vec![row("d0", 0, 0.0)]).await;
    h.send(vec![row("d1", 1, 0.0)]).await;
    h.send(vec![row("d2", 2, 0.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (2, 1));
    assert_eq!(
        (snap.redis_sink_command_errors, snap.redis_sink_fatal),
        (1, 0)
    );
    assert_eq!(server.cmd(&["HGET", "h:d2", "n"]).text(), "2");
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_tls_acl_auth_and_rejections() {
    let Some(server) = Server::start() else {
        return;
    };
    let start = |user: &str, password: &str, command: RedisCommand| {
        let mut c = sink_config(server.tls_url(), command);
        tls(&mut c.target, Some(user));
        c.target.password_secret = Some(password.into());
        c.target.url = format!("{}/3", server.tls_url());
        let ep = c.target.endpoint().unwrap();
        let diag = crate::diag::IoDiagnostics::new();
        let owner = owner();
        diag.observation.initialize(&owner).unwrap();
        let sink = RedisSink::bind(
            c,
            &secrets(user),
            &TargetPolicy::allow(ep.host, ep.port),
            owner.clone(),
            diag.clone(),
        )
        .unwrap();
        (sink, owner, diag)
    };
    async fn drive(
        sink: RedisSink,
        owner: Arc<MemoryOwner>,
        diag: Arc<crate::diag::IoDiagnostics>,
    ) -> (u64, u64, crate::diag::IoSnapshot, bool) {
        let (tx, rx) = sparrow_io::observed::channel(4);
        let outbox = Arc::new(sparrow_model::InflightCounter::new());
        let cancel = CancellationToken::new();
        let task = tokio::spawn(sink.run(rx, cancel.clone(), Some(outbox.clone())));
        outbox.enqueue();
        tx.send(super::tests::batch(&owner, vec![row("t", 1, 1.0)]))
            .await
            .unwrap();
        drop(tx);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        (
            outbox.acked(),
            outbox.failed(),
            diag.snapshot(),
            cancel.is_cancelled(),
        )
    }
    let (sink, o, d) = start("app", "pw", set_cmd());
    let (acked, _, _, _) = drive(sink, o, d).await;
    assert_eq!(acked, 1);
    server.cmd(&["SELECT", "3"]);
    let V::Array(Some(_)) = server.cmd(&["KEYS", "*"]) else {
        panic!()
    };
    // Written to db 3, not db 0.
    assert_eq!(server.cmd(&["GET", "dev:t"]), V::Bulk(None));
    // Wrong password: rejected at the handshake, job fails, no retries.
    let (sink, o, d) = start("app", "bad", set_cmd());
    let (_, failed, snap, cancelled) = drive(sink, o, d).await;
    assert!(cancelled);
    assert_eq!(
        (failed, snap.redis_sink_fatal, snap.redis_sink_retries),
        (1, 1, 0)
    );
    // ACL: the read-only user may not SET -> NOPERM is fatal.
    let (sink, o, d) = start("reader", "pw", set_cmd());
    let (_, failed, snap, cancelled) = drive(sink, o, d).await;
    assert!(cancelled);
    assert_eq!((failed, snap.redis_sink_fatal), (1, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_reconnects_after_the_server_drops_it() {
    let Some(server) = Server::start() else {
        return;
    };
    let mut c = sink_config(
        server.url(),
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
    // Kill every normal client (the sink's idle connection).
    assert!(
        server
            .cmd(&["CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"])
            .int()
            >= 1
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.send(vec![row("b", 2, 1.0)]).await;
    let (outbox, snap) = h.finish().await;
    assert_eq!((outbox.acked(), outbox.failed()), (2, 0));
    assert_eq!(
        (snap.redis_sink_connects, snap.redis_sink_unknown_outcome),
        (2, 0)
    );
    assert_eq!(
        server.cmd(&["XLEN", "s"]).int(),
        2,
        "each XADD exactly once"
    );
}

// ------------------------------------------------------- lookup tests --

/// The same thin adapter the control plane uses (`RedisProvider`).
struct Provider(RedisLookup);

impl ExternalLookup for Provider {
    fn schema(&self) -> &Schema {
        self.0.schema()
    }
    fn keys(&self) -> &[String] {
        self.0.keys()
    }
    fn scratch_bytes(&self) -> usize {
        self.0.scratch_bytes()
    }
    fn max_batch_keys(&self) -> usize {
        self.0.max_batch_keys()
    }
    fn lookup<'a>(
        &'a self,
        key: Vec<Scalar>,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + 'a>> {
        Box::pin(self.0.lookup(key, cancel))
    }
    fn lookup_batch<'a>(
        &'a self,
        keys: Vec<Vec<Scalar>>,
        cancel: CancellationToken,
    ) -> sparrow_runtime::external_lookup::LookupBatchFuture<'a> {
        Box::pin(self.0.lookup_batch(keys, cancel))
    }
}

fn stream_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "s", DataType::Utf8, true),
            Field::new(FieldId::new(2), "d", DataType::Int64, true),
        ],
    )
    .unwrap()
}

fn stream(owner: &Arc<MemoryOwner>, keys: &[(&str, i64)]) -> RowBatch {
    let mut b = RowBatchBuilder::new(
        Arc::new(stream_schema()),
        owner.clone(),
        CreditKind::Reservation,
        keys.len().max(1),
        1 << 20,
    )
    .unwrap();
    for (s, d) in keys {
        b.push(Row {
            values: vec![Scalar::utf8(*s), Scalar::Int64(*d)],
        })
        .unwrap();
    }
    b.finish().unwrap()
}

fn operator(
    config: RedisLookupConfig,
    user: &str,
    options: ExternalLookupOptions,
    owner: &Arc<MemoryOwner>,
) -> ExternalLookupOperator {
    let ep = config.target.endpoint().unwrap();
    let lookup = RedisLookup::bind(
        config,
        &secrets(user),
        &TargetPolicy::allow(ep.host, ep.port),
    )
    .unwrap();
    ExternalLookupOperator::new(
        LookupSpec::static_table(
            "limits",
            vec!["s".into(), "d".into()],
            vec!["site".into(), "dev".into()],
            vec!["limit".into(), "label".into()],
        ),
        ExternalLookupBinding {
            provider: Arc::new(Provider(lookup)),
            options,
        },
        stream_schema(),
        owner.clone(),
    )
    .unwrap()
}

fn limits(batch: &RowBatch) -> Vec<(Scalar, Scalar)> {
    batch
        .rows()
        .iter()
        .map(|r| (r.values[2].clone(), r.values[3].clone()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn real_lookup_hits_misses_batches_and_ttl() {
    let Some(server) = Server::start() else {
        return;
    };
    for d in 0..6 {
        server.cmd(&[
            "HSET",
            &format!("lim:a:{d}"),
            "limit",
            &format!("{d}.5"),
            "label",
            &format!("L{d}"),
        ]);
    }
    // 8 keys in flight need (256 + 65) KiB each: more than half of the
    // compact 4 MiB reservation, so use the performance budget.
    let owner = MemoryOwner::new(ResourceBudget::performance());
    let mut op = operator(
        lookup_config(server.url(), RedisLookupFormat::Hash),
        "app",
        ExternalLookupOptions {
            max_inflight: 2,
            batch_keys: 4,
            cache_ttl_ms: 300,
            ..Default::default()
        },
        &owner,
    );
    let keys = [
        ("a", 0),
        ("a", 1),
        ("x", 1),
        ("a", 2),
        ("a", 3),
        ("a", 4),
        ("a", 5),
    ];
    let input = stream(&owner, &keys);
    let out = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.rows().len(), 7, "left join keeps misses");
    assert_eq!(limits(&out)[1], (Scalar::Float64(1.5), Scalar::utf8("L1")));
    assert_eq!(limits(&out)[2], (Scalar::Null, Scalar::Null));
    let snap = op.diagnostics().snapshot();
    assert_eq!(
        (snap.requests, snap.hits, snap.misses),
        (2, 6, 1),
        "7 keys in 2 pipelined requests"
    );

    drop(out);
    // Served from cache (incl. the negative entry) despite a changed value.
    server.cmd(&["HSET", "lim:a:1", "limit", "99"]);
    server.cmd(&["HSET", "lim:x:1", "limit", "7"]);
    let out = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(limits(&out)[1].0, Scalar::Float64(1.5));
    let snap = op.diagnostics().snapshot();
    // cache_hits counts negative hits as well.
    assert_eq!(
        (snap.requests, snap.cache_hits, snap.negative_cache_hits),
        (2, 7, 1)
    );

    drop(out);
    // After the TTL the new values are fetched.
    tokio::time::sleep(Duration::from_millis(350)).await;
    let out = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(limits(&out)[1].0, Scalar::Float64(99.0));
    assert_eq!(limits(&out)[2].0, Scalar::Float64(7.0));
    assert_eq!(op.diagnostics().snapshot().requests, 4);
    drop((out, input, op));
    assert_eq!(
        owner.usage().physical_bytes,
        0,
        "cache and scratch released"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn real_lookup_cache_is_bounded_by_bytes() {
    let Some(server) = Server::start() else {
        return;
    };
    for d in 0..40 {
        server.cmd(&[
            "HSET",
            &format!("lim:a:{d}"),
            "limit",
            "1",
            "label",
            &"x".repeat(200),
        ]);
    }
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(
        lookup_config(server.url(), RedisLookupFormat::Hash),
        "app",
        ExternalLookupOptions {
            cache_bytes: 4096,
            cache_ttl_ms: 60_000,
            ..Default::default()
        },
        &owner,
    );
    let keys: Vec<(&str, i64)> = (0..40).map(|d| ("a", d)).collect();
    let input = stream(&owner, &keys);
    op.on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap();
    let snap = op.diagnostics().snapshot();
    assert!(snap.cache_bytes <= 4096, "{}", snap.cache_bytes);
    assert!(
        snap.cache_entries > 0 && snap.cache_entries < 40,
        "{}",
        snap.cache_entries
    );
    // Re-reading the first keys misses the cache: they were evicted.
    op.on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap();
    let after = op.diagnostics().snapshot();
    assert!(
        after.requests > snap.requests + 30,
        "{} -> {}",
        snap.requests,
        after.requests
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn real_lookup_timeout_policy_and_reconnect() {
    let Some(server) = Server::start() else {
        return;
    };
    server.cmd(&["HSET", "lim:a:1", "limit", "1"]);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let options = |on_error| ExternalLookupOptions {
        timeout_ms: 150,
        cache_bytes: 0,
        on_error,
        ..Default::default()
    };
    let mut fail = operator(
        lookup_config(server.url(), RedisLookupFormat::Hash),
        "app",
        options(LookupErrorPolicy::Fail),
        &owner,
    );
    let mut null = operator(
        lookup_config(server.url(), RedisLookupFormat::Hash),
        "app",
        options(LookupErrorPolicy::Null),
        &owner,
    );
    let input = stream(&owner, &[("a", 1)]);
    // Warm both pools, then kill their idle connections: the next lookup
    // retries once on a fresh connection and succeeds.
    fail.on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    null.on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert!(
        server
            .cmd(&["CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"])
            .int()
            >= 2
    );
    let out = fail
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(limits(&out)[0].0, Scalar::Float64(1.0));

    // Paused server: the lookup deadline fires.
    server.cmd(&["CLIENT", "PAUSE", "1000"]);
    let started = Instant::now();
    let err = fail
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::JobFailed, "{}", err.message);
    let out = null
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(limits(&out)[0].0, Scalar::Null);
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(fail.diagnostics().snapshot().timeouts, 1);
    assert_eq!(null.diagnostics().snapshot().error_nulls, 1);
    server.cmd(&["CLIENT", "UNPAUSE"]);
    // Recovers after the pause.
    let out = fail
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(limits(&out)[0].0, Scalar::Float64(1.0));
}

#[tokio::test(flavor = "multi_thread")]
async fn real_lookup_json_over_tls_with_acl_user() {
    let Some(server) = Server::start() else {
        return;
    };
    server.cmd(&[
        "SET",
        "lim:a:1",
        r#"{"site":"a","dev":1,"limit":2.5,"label":"pump"}"#,
    ]);
    server.cmd(&["SET", "lim:a:2", "not json"]);
    let mut config = lookup_config(server.tls_url(), RedisLookupFormat::Json);
    tls(&mut config.target, Some("reader"));
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut op = operator(
        config.clone(),
        "reader",
        ExternalLookupOptions::default(),
        &owner,
    );
    let out = op
        .on_batch_into(
            &stream(&owner, &[("a", 1), ("b", 1)]),
            CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        limits(&out),
        vec![
            (Scalar::Float64(2.5), Scalar::utf8("pump")),
            (Scalar::Null, Scalar::Null)
        ]
    );
    // Undecodable values are hard errors, never NULL.
    let mut null = operator(
        config.clone(),
        "reader",
        ExternalLookupOptions {
            on_error: LookupErrorPolicy::Null,
            ..Default::default()
        },
        &owner,
    );
    let err = null
        .on_batch_into(&stream(&owner, &[("a", 2)]), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::CodecViolation);
    // Wrong password -> PolicyDenied (hard).
    config.target.password_secret = Some("bad".into());
    let mut bad = operator(config, "reader", ExternalLookupOptions::default(), &owner);
    let err = bad
        .on_batch_into(&stream(&owner, &[("a", 1)]), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PolicyDenied);
    let _ = key("a", 1);
}

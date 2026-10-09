//! Opt-in tests against a real PostgreSQL 16 server.
//!
//! Set `SPARROW_POSTGRES_BIN` to the `bin` directory of a PostgreSQL 16.x
//! installation built with OpenSSL (`initdb`, `postgres`); every test is a
//! no-op otherwise. Each test initialises its own cluster in a temporary
//! directory on a free port, with TLS (the test CA's `localhost`
//! certificate) and these roles:
//!
//! * `postgres`: superuser, `trust` (administration from the test);
//! * `plain`: `trust` over plain TCP (sslmode=disable paths, proxies);
//! * `app_pw`: password `s3cret-pw`, SCRAM, only over TLS (`hostssl`).

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sparrow_model::{
    CreditKind, DataType, ErrorCode, InflightCounter, MemoryOwner, QueuedRow, ResourceBudget,
    Result, Row, RowBatch, RowBatchBuilder, Scalar, Schema,
};
use sparrow_plan::LookupSpec;
use sparrow_runtime::external_lookup::ExternalLookupOperator;
use sparrow_runtime::{ExternalLookup, ExternalLookupBinding, ExternalLookupOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::conn::PgConn;
use super::tests::{field, schema};
use super::tls_fixture;
use super::*;
use crate::diag::IoDiagnostics;
use crate::{MapSecretResolver, TargetPolicy};

const PASSWORD: &str = "s3cret-pw";

// ------------------------------------------------------------- server --

struct Server {
    child: Child,
    dir: PathBuf,
    port: u16,
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

fn secrets() -> MapSecretResolver {
    MapSecretResolver::new(
        [
            ("pg_pw".to_string(), PASSWORD.to_string()),
            ("pg_wrong".to_string(), "not-the-password".to_string()),
        ]
        .into(),
    )
}

impl Server {
    async fn start() -> Option<Self> {
        let Some(bin) = std::env::var_os("SPARROW_POSTGRES_BIN").map(PathBuf::from) else {
            eprintln!("SPARROW_POSTGRES_BIN not set; skipping real PostgreSQL test");
            return None;
        };
        let version = Command::new(bin.join("postgres"))
            .arg("--version")
            .output()
            .expect("postgres --version");
        let version = String::from_utf8_lossy(&version.stdout).into_owned();
        assert!(
            version.contains(" 16."),
            "PostgreSQL 16.x expected, got {version}"
        );
        let dir =
            std::env::temp_dir().join(format!("sparrow-pg-{}-{}", std::process::id(), free_port()));
        let data = dir.join("data");
        std::fs::create_dir_all(&dir).unwrap();
        let out = Command::new(bin.join("initdb"))
            .args(["-D"])
            .arg(&data)
            .args([
                "-U",
                "postgres",
                "-A",
                "trust",
                "-E",
                "UTF8",
                "--locale=C",
                "--no-sync",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .expect("initdb");
        assert!(
            out.status.success(),
            "initdb: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::fs::write(dir.join("server.crt"), tls_fixture::CERT).unwrap();
        std::fs::write(dir.join("server.key"), tls_fixture::KEY).unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                dir.join("server.key"),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        std::fs::write(
            data.join("pg_hba.conf"),
            "local all all trust\n\
             hostssl all app_pw all scram-sha-256\n\
             host all app_pw all reject\n\
             host all postgres all trust\n\
             host all plain all trust\n",
        )
        .unwrap();
        for _ in 0..5 {
            let port = free_port();
            let conf = format!(
                "listen_addresses = 'localhost'\nport = {port}\nunix_socket_directories = '{d}'\n\
                 ssl = on\nssl_cert_file = '{d}/server.crt'\nssl_key_file = '{d}/server.key'\n\
                 fsync = off\nsynchronous_commit = off\nmax_connections = 50\n",
                d = dir.display()
            );
            std::fs::write(data.join("postgresql.auto.conf"), conf).unwrap();
            let log = std::fs::File::create(dir.join("server.log")).unwrap();
            let child = Command::new(bin.join("postgres"))
                .arg("-D")
                .arg(&data)
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .expect("spawn postgres");
            let mut server = Self {
                child,
                dir: dir.clone(),
                port,
            };
            let started = Instant::now();
            loop {
                if server.try_admin().await.is_some() {
                    server.setup().await;
                    return Some(server);
                }
                if server.child.try_wait().unwrap().is_some() {
                    break;
                }
                assert!(
                    started.elapsed() < Duration::from_secs(20),
                    "postgres did not start: {}",
                    std::fs::read_to_string(dir.join("server.log")).unwrap_or_default()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            server.dir = PathBuf::new();
        }
        panic!("postgres did not start");
    }

    fn plain_target(&self, user: &str) -> PgTarget {
        let mut t = PgTarget::new(format!("postgresql://127.0.0.1:{}/app", self.port), user);
        t.sslmode = PgSslMode::Disable;
        t
    }

    fn tls_target(&self) -> PgTarget {
        let mut t = PgTarget::new(
            format!("postgresql://localhost:{}/app", self.port),
            "app_pw",
        );
        t.password_secret = Some("pg_pw".into());
        t.ca_pem = Some(String::from_utf8_lossy(tls_fixture::CA).into_owned());
        t
    }

    fn policy(&self) -> TargetPolicy {
        TargetPolicy::allow("127.0.0.1", self.port).with_allow("localhost", self.port)
    }

    async fn try_admin(&self) -> Option<PgConn> {
        let mut t = self.plain_target("postgres");
        t.url = format!("postgresql://127.0.0.1:{}/postgres", self.port);
        t.bind(&secrets(), &self.policy(), 1 << 20)
            .unwrap()
            .connect()
            .await
            .ok()
    }

    async fn admin(&self) -> PgConn {
        let mut t = self.plain_target("postgres");
        t.url = format!("postgresql://127.0.0.1:{}/app", self.port);
        t.bind(&secrets(), &self.policy(), 1 << 20)
            .unwrap()
            .connect()
            .await
            .expect("admin connection")
    }

    async fn setup(&self) {
        let admin = self.try_admin().await.unwrap();
        // CREATE DATABASE cannot share an implicit transaction block.
        for sql in [
            "CREATE DATABASE app".to_string(),
            "CREATE ROLE plain LOGIN".to_string(),
            format!("CREATE ROLE app_pw LOGIN PASSWORD '{PASSWORD}'"),
        ] {
            admin.client.batch_execute(&sql).await.unwrap();
        }
        drop(admin);
        let admin = self.admin().await;
        admin
            .client
            .batch_execute(
                "GRANT ALL ON SCHEMA public TO plain, app_pw;\
                 ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO plain, app_pw;",
            )
            .await
            .unwrap();
    }

    async fn exec(&self, sql: &str) {
        self.admin()
            .await
            .client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }

    async fn count(&self, sql: &str) -> i64 {
        self.admin()
            .await
            .client
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .get::<_, i64>(0)
    }

    /// Statements of `user` waiting on a lock or running.
    async fn busy(&self, user: &str) -> i64 {
        self.count(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE usename = '{user}' AND state = 'active'"
        ))
        .await
    }
}

async fn until<'a>(
    limit: Duration,
    mut f: impl FnMut() -> Pin<Box<dyn Future<Output = bool> + 'a>>,
) {
    let start = Instant::now();
    while !f().await {
        assert!(
            start.elapsed() < limit,
            "condition not reached in {limit:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// --------------------------------------------------------- connection --

#[tokio::test(flavor = "multi_thread")]
async fn real_tls_scram_auth_and_refusals() {
    let Some(server) = Server::start().await else {
        return;
    };
    // verify-full with the test CA and SCRAM password.
    let conn = server
        .tls_target()
        .bind(&secrets(), &server.policy(), 1 << 20)
        .unwrap()
        .connect()
        .await
        .expect("TLS + SCRAM");
    let row = conn
        .client
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .unwrap();
    assert!(row.get::<_, bool>(0), "session is encrypted");
    // Wrong password: rejected, not retryable.
    let mut t = server.tls_target();
    t.password_secret = Some("pg_wrong".into());
    match t
        .bind(&secrets(), &server.policy(), 1 << 20)
        .unwrap()
        .connect()
        .await
    {
        Err(conn::ConnectError::Rejected(ErrorCode::PolicyDenied, "postgres_auth_rejected")) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
    // Built-in web roots do not trust the test CA.
    let mut t = server.tls_target();
    t.ca_pem = None;
    match t
        .bind(&secrets(), &server.policy(), 1 << 20)
        .unwrap()
        .connect()
        .await
    {
        Err(conn::ConnectError::Io(_)) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
    // The certificate names localhost, not 127.0.0.1.
    let mut t = server.tls_target();
    t.url = format!("postgresql://127.0.0.1:{}/app", server.port);
    match t
        .bind(&secrets(), &server.policy(), 1 << 20)
        .unwrap()
        .connect()
        .await
    {
        Err(conn::ConnectError::Io(_)) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
    // app_pw is hostssl-only: a plain connection is refused by pg_hba.
    let mut t = server.plain_target("app_pw");
    t.url = format!("postgresql://127.0.0.1:{}/app", server.port);
    match t
        .bind(&secrets(), &server.policy(), 1 << 20)
        .unwrap()
        .connect()
        .await
    {
        Err(conn::ConnectError::Rejected(ErrorCode::PolicyDenied, _)) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
    // Missing database.
    let mut t = server.plain_target("plain");
    t.url = format!("postgresql://127.0.0.1:{}/nope", server.port);
    match t
        .bind(&secrets(), &server.policy(), 1 << 20)
        .unwrap()
        .connect()
        .await
    {
        Err(conn::ConnectError::Rejected(
            ErrorCode::InvalidArgument,
            "postgres_database_missing",
        )) => {}
        other => panic!("{:?}", other.map(|_| ())),
    }
    // A backend message over the bound fails the connection on read.
    let conn = server
        .plain_target("plain")
        .bind(&secrets(), &server.policy(), 64 * 1024)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let e = conn
        .client
        .query_one("SELECT repeat('x', 100000)", &[])
        .await
        .unwrap_err();
    assert!(e.as_db_error().is_none(), "{e:?}");
    assert!(conn.is_closed());
}

// --------------------------------------------------------------- sink --

struct SinkHarness {
    tx: Option<sparrow_io::observed::Sender<RowBatch>>,
    outbox: Arc<InflightCounter>,
    cancel: CancellationToken,
    diag: Arc<IoDiagnostics>,
    owner: Arc<MemoryOwner>,
    schema: Arc<Schema>,
    task: tokio::task::JoinHandle<()>,
}

impl SinkHarness {
    fn start(server: &Server, config: PgSinkConfig, schema: Schema) -> Self {
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let diag = IoDiagnostics::new();
        diag.observation.initialize(&owner).unwrap();
        let sink = PgSink::bind(
            config,
            &secrets(),
            &server.policy(),
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
            schema: Arc::new(schema),
            task,
        }
    }

    async fn send(&self, rows: Vec<Row>) {
        let mut b = RowBatchBuilder::new(
            self.schema.clone(),
            self.owner.clone(),
            CreditKind::Reservation,
            rows.len().max(1),
            1 << 20,
        )
        .unwrap();
        for r in rows {
            b.push(r).unwrap();
        }
        self.outbox.enqueue();
        self.tx
            .as_ref()
            .unwrap()
            .send(b.finish().unwrap())
            .await
            .unwrap();
    }

    async fn settled(&self, n: u64) -> (u64, u64) {
        tokio::time::timeout(Duration::from_secs(20), async {
            while self.outbox.acked() + self.outbox.failed() < n {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("receipts settle");
        (self.outbox.acked(), self.outbox.failed())
    }

    async fn finish(mut self) -> crate::diag::IoSnapshot {
        drop(self.tx.take());
        tokio::time::timeout(Duration::from_secs(10), &mut self.task)
            .await
            .expect("sink finishes")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.owner.usage().reservation_bytes != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("sink releases its reservation");
        self.diag.snapshot()
    }
}

fn all_types_schema() -> Schema {
    schema(vec![
        field(1, "id", DataType::Int64, false),
        field(2, "i2", DataType::Int64, true),
        field(3, "i4", DataType::Int64, true),
        field(4, "f4", DataType::Float64, true),
        field(5, "f8", DataType::Float64, true),
        field(6, "num_text", DataType::Utf8, true),
        field(7, "num_float", DataType::Float64, true),
        field(8, "t", DataType::Utf8, true),
        field(9, "vc", DataType::Utf8, true),
        field(10, "b", DataType::Bool, true),
        field(11, "ts", DataType::TimestampMicrosUTC, true),
        field(12, "tstz", DataType::TimestampMicrosUTC, true),
        field(13, "j", DataType::Utf8, true),
        field(14, "jb", DataType::Utf8, true),
        field(15, "by", DataType::Bytes, true),
    ])
}

const ALL_TYPES_TABLE: &str =
    "CREATE TABLE typed (id int8 PRIMARY KEY, i2 int2, i4 int4, f4 float4, f8 float8, \
     num_text numeric(20,4), num_float numeric, t text, vc varchar(8), b bool, ts timestamp, \
     tstz timestamptz, j json, jb jsonb, by bytea)";

fn typed_row(id: i64) -> Row {
    Row {
        values: vec![
            Scalar::Int64(id),
            Scalar::Int64(-32768),
            Scalar::Int64(2_147_483_647),
            Scalar::Float64(1.5),
            Scalar::Float64(-0.1),
            Scalar::utf8("12345.6789"),
            Scalar::Float64(0.25),
            Scalar::utf8("héllo"),
            Scalar::utf8("short"),
            Scalar::Bool(true),
            // 2024-01-02T03:04:05.000006Z
            Scalar::TimestampMicrosUTC(1_704_164_645_000_006),
            Scalar::TimestampMicrosUTC(-1),
            Scalar::utf8("{\"a\": [1, 2]}"),
            Scalar::utf8("{\"b\": null}"),
            Scalar::Bytes(vec![0u8, 255, 10].into()),
        ],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_types_upsert_idempotency_and_bad_rows() {
    let Some(server) = Server::start().await else {
        return;
    };
    server.exec(ALL_TYPES_TABLE).await;
    let s = all_types_schema();
    let mut config = PgSinkConfig::new(
        server.tls_target(),
        "typed",
        PgWriteMode::Upsert {
            conflict_key: vec!["id".into()],
            update_columns: vec!["t".into(), "f8".into()],
        },
    );
    config.chunk_rows = 2;
    let h = SinkHarness::start(&server, config, s.clone());
    h.send((1..=5).map(typed_row).collect()).await;
    assert_eq!(h.settled(1).await, (1, 0));
    // Exact values, read back with the server's own text output.
    let admin = server.admin().await;
    let row = admin
        .client
        .query_one(
            "SELECT i2::text, i4::text, f4::text, f8::text, num_text::text, num_float::text, t, vc, b::text, \
             ts::text, (tstz AT TIME ZONE 'UTC')::text, j::text, jb::text, encode(by, 'hex') FROM typed WHERE id = 3",
            &[],
        )
        .await
        .unwrap();
    let got: Vec<String> = (0..14).map(|i| row.get::<_, String>(i)).collect();
    assert_eq!(
        got,
        [
            "-32768",
            "2147483647",
            "1.5",
            "-0.1",
            "12345.6789",
            "0.25",
            "héllo",
            "short",
            "true",
            "2024-01-02 03:04:05.000006",
            "1969-12-31 23:59:59.999999",
            "{\"a\": [1, 2]}",
            "{\"b\": null}",
            "00ff0a"
        ]
    );
    // Re-sending is idempotent; a key repeated within one batch applies in
    // order (last value wins, no "affect row a second time" error).
    let mut a = typed_row(1);
    a.values[7] = Scalar::utf8("first");
    let mut b = typed_row(1);
    b.values[7] = Scalar::utf8("second");
    h.send((1..=5).map(typed_row).chain([a, b]).collect()).await;
    assert_eq!(h.settled(2).await, (2, 0));
    assert_eq!(server.count("SELECT count(*) FROM typed").await, 5);
    assert_eq!(
        server
            .count("SELECT count(*) FROM typed WHERE id = 1 AND t = 'second'")
            .await,
        1
    );
    // Values that do not fit are dropped (batch fails), the rest commit.
    let mut bad_range = typed_row(10);
    bad_range.values[1] = Scalar::Int64(40_000);
    let mut bad_nul = typed_row(11);
    bad_nul.values[7] = Scalar::utf8("a\0b");
    let mut bad_json = typed_row(12);
    bad_json.values[13] = Scalar::utf8("{nope");
    let mut null_key = typed_row(13);
    null_key.values[0] = Scalar::Null;
    h.send(vec![bad_range, typed_row(14), bad_nul, bad_json])
        .await;
    assert_eq!(h.settled(3).await, (2, 1));
    assert_eq!(server.count("SELECT count(*) FROM typed").await, 6);
    // A server-side data error (varchar(8) overflow) rolls the whole batch
    // back and is not retried.
    let mut too_long = typed_row(20);
    too_long.values[8] = Scalar::utf8("way too long");
    h.send(vec![typed_row(21), too_long]).await;
    assert_eq!(h.settled(4).await, (2, 2));
    assert_eq!(
        server
            .count("SELECT count(*) FROM typed WHERE id IN (20, 21)")
            .await,
        0
    );
    let snap = h.finish().await;
    assert_eq!(snap.postgres_sink_dropped_bad, 3);
    assert_eq!(snap.postgres_sink_batch_errors, 1);
    assert_eq!(snap.postgres_sink_retries, 0);
    assert_eq!(snap.postgres_sink_rows, 5 + 7 + 1);
    // 5 rows in chunks of 2 = 3 statements; 7 rows: 1,2 | 3,4 | 5,1 | 1(dup) = 4.
    assert!(
        snap.postgres_sink_statements > 3 + 4,
        "{}",
        snap.postgres_sink_statements
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_schema_and_permission_failures_stop_the_job() {
    let Some(server) = Server::start().await else {
        return;
    };
    server
        .exec("CREATE TABLE narrow (id int8 PRIMARY KEY, n uuid); CREATE TABLE locked (id int8); REVOKE ALL ON locked FROM plain")
        .await;
    let s = schema(vec![
        field(1, "id", DataType::Int64, false),
        field(2, "n", DataType::Utf8, true),
    ]);
    for (table, reason) in [
        ("narrow", "postgres_column_type_unsupported"),
        ("missing", "postgres_table_missing"),
    ] {
        let h = SinkHarness::start(
            &server,
            PgSinkConfig::new(server.plain_target("plain"), table, PgWriteMode::Insert),
            s.clone(),
        );
        h.send(vec![Row {
            values: vec![Scalar::Int64(1), Scalar::utf8("x")],
        }])
        .await;
        h.settled(1).await;
        tokio::time::timeout(Duration::from_secs(5), h.cancel.cancelled())
            .await
            .unwrap_or_else(|_| panic!("{table}: sink stops the job"));
        let snap = h.finish().await;
        assert_eq!(snap.postgres_sink_fatal, 1, "{table} {reason}");
    }
    let s = schema(vec![field(1, "id", DataType::Int64, false)]);
    let h = SinkHarness::start(
        &server,
        PgSinkConfig::new(server.plain_target("plain"), "locked", PgWriteMode::Insert),
        s,
    );
    h.send(vec![Row {
        values: vec![Scalar::Int64(1)],
    }])
    .await;
    h.settled(1).await;
    tokio::time::timeout(Duration::from_secs(5), h.cancel.cancelled())
        .await
        .expect("permission denied stops the job");
    assert_eq!(h.finish().await.postgres_sink_fatal, 1);
}

/// TCP relay that, once armed, closes both sides right after forwarding a
/// client chunk containing `COMMIT` (before the reply can come back).
struct CutProxy {
    port: u16,
    armed: Arc<AtomicBool>,
}

impl CutProxy {
    async fn start(upstream: u16) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let armed = Arc::new(AtomicBool::new(false));
        let flag = armed.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                let Ok(server) = tokio::net::TcpStream::connect(("127.0.0.1", upstream)).await
                else {
                    return;
                };
                let flag = flag.clone();
                tokio::spawn(async move {
                    let (mut cr, mut cw) = client.into_split();
                    let (mut sr, mut sw) = server.into_split();
                    let down = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut sr, &mut cw).await;
                    });
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = match cr.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        if sw.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                        if buf[..n].windows(6).any(|w| w == b"COMMIT")
                            && flag.swap(false, Ordering::SeqCst)
                        {
                            down.abort();
                            return;
                        }
                    }
                    down.abort();
                });
            }
        });
        Self { port, armed }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_commit_outcome_unknown_insert_vs_upsert() {
    let Some(server) = Server::start().await else {
        return;
    };
    server
        .exec("CREATE TABLE ins (id int8, v text); CREATE TABLE ups (id int8 PRIMARY KEY, v text)")
        .await;
    let proxy = CutProxy::start(server.port).await;
    let s = schema(vec![
        field(1, "id", DataType::Int64, false),
        field(2, "v", DataType::Utf8, true),
    ]);
    let rows = || {
        (1..=3)
            .map(|i| Row {
                values: vec![Scalar::Int64(i), Scalar::utf8("v")],
            })
            .collect::<Vec<_>>()
    };
    let mut target = server.plain_target("plain");
    target.url = format!("postgresql://127.0.0.1:{}/app", proxy.port);
    let policy_ok = |c: &mut PgSinkConfig| {
        c.retry_initial = Duration::from_millis(10);
        c.retry_max = Duration::from_millis(100);
    };
    // INSERT: the COMMIT reply is lost -> unknown outcome, not re-sent.
    let mut config = PgSinkConfig::new(target.clone(), "ins", PgWriteMode::Insert);
    policy_ok(&mut config);
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let diag = IoDiagnostics::new();
    diag.observation.initialize(&owner).unwrap();
    let policy = TargetPolicy::allow("127.0.0.1", proxy.port);
    let sink = PgSink::bind(config, &secrets(), &policy, owner.clone(), diag.clone()).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(4);
    let outbox = Arc::new(InflightCounter::new());
    let task = tokio::spawn(sink.run(rx, CancellationToken::new(), Some(outbox.clone())));
    let batch = |rows: Vec<Row>| {
        let mut b = RowBatchBuilder::new(
            Arc::new(s.clone()),
            owner.clone(),
            CreditKind::Reservation,
            4,
            1 << 20,
        )
        .unwrap();
        for r in rows {
            b.push(r).unwrap();
        }
        b.finish().unwrap()
    };
    proxy.armed.store(true, Ordering::SeqCst);
    outbox.enqueue();
    tx.send(batch(rows())).await.unwrap();
    drop(tx);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((outbox.acked(), outbox.failed()), (0, 1));
    let snap = diag.snapshot();
    assert_eq!(
        (
            snap.postgres_sink_unknown_outcome,
            snap.postgres_sink_retries
        ),
        (3, 0)
    );
    let n = server.count("SELECT count(*) FROM ins").await;
    assert!(n == 0 || n == 3, "all or nothing, never twice: {n}");
    // UPSERT: the same loss is retried; the rows end up exactly once.
    let mut config = PgSinkConfig::new(
        target,
        "ups",
        PgWriteMode::Upsert {
            conflict_key: vec!["id".into()],
            update_columns: vec!["v".into()],
        },
    );
    policy_ok(&mut config);
    let diag = IoDiagnostics::new();
    diag.observation.initialize(&owner).unwrap();
    let sink = PgSink::bind(config, &secrets(), &policy, owner.clone(), diag.clone()).unwrap();
    let (tx, rx) = sparrow_io::observed::channel(4);
    let outbox = Arc::new(InflightCounter::new());
    let task = tokio::spawn(sink.run(rx, CancellationToken::new(), Some(outbox.clone())));
    proxy.armed.store(true, Ordering::SeqCst);
    outbox.enqueue();
    tx.send(batch(rows())).await.unwrap();
    drop(tx);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((outbox.acked(), outbox.failed()), (1, 0));
    let snap = diag.snapshot();
    assert_eq!(
        (snap.postgres_sink_retries, snap.postgres_sink_connects),
        (1, 2)
    );
    assert_eq!(server.count("SELECT count(*) FROM ups").await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn real_sink_reconnects_and_stop_cancels_in_flight_statement() {
    let Some(server) = Server::start().await else {
        return;
    };
    server.exec("CREATE TABLE r (id int8 PRIMARY KEY)").await;
    let s = schema(vec![field(1, "id", DataType::Int64, false)]);
    let mut config = PgSinkConfig::new(server.plain_target("plain"), "r", PgWriteMode::Insert);
    config.retry_initial = Duration::from_millis(10);
    config.flush_timeout = Duration::from_millis(300);
    let h = SinkHarness::start(&server, config, s);
    h.send(vec![Row {
        values: vec![Scalar::Int64(1)],
    }])
    .await;
    assert_eq!(h.settled(1).await, (1, 0));
    // The server ends the session between batches: reconnect, nothing lost.
    server
        .exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'plain'")
        .await;
    h.send(vec![Row {
        values: vec![Scalar::Int64(2)],
    }])
    .await;
    assert_eq!(h.settled(2).await, (2, 0));
    assert_eq!(server.count("SELECT count(*) FROM r").await, 2);
    // A batch blocked on a lock: stop must end within flush_timeout and the
    // server-side statement must be cancelled (not left waiting).
    let locker = server.admin().await;
    locker
        .client
        .batch_execute("BEGIN; LOCK TABLE r IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    h.send(vec![Row {
        values: vec![Scalar::Int64(3)],
    }])
    .await;
    until(Duration::from_secs(5), || {
        Box::pin(async { server.busy("plain").await == 1 })
    })
    .await;
    let stopped = Instant::now();
    h.cancel.cancel();
    let snap = h.finish().await;
    assert!(
        stopped.elapsed() < Duration::from_secs(3),
        "{:?}",
        stopped.elapsed()
    );
    until(Duration::from_secs(5), || {
        Box::pin(async { server.busy("plain").await == 0 })
    })
    .await;
    locker.client.batch_execute("COMMIT").await.unwrap();
    assert_eq!(server.count("SELECT count(*) FROM r").await, 2);
    assert_eq!(snap.postgres_sink_connects, 2);
    assert_eq!(snap.postgres_sink_discarded_on_close, 1);
}

// ------------------------------------------------------------- source --

struct SourceHarness {
    rx: sparrow_io::observed::Receiver<QueuedRow>,
    owner: Arc<MemoryOwner>,
    schema: Arc<Schema>,
    diag: Arc<IoDiagnostics>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl SourceHarness {
    fn start(server: &Server, config: PgSourceConfig, max_row_bytes: usize) -> Self {
        let schema = Arc::new(config.schema.clone());
        let diag = IoDiagnostics::new();
        let capacity = config.inbox_capacity;
        let source = PgSource::bind(
            config,
            &secrets(),
            &server.policy(),
            max_row_bytes,
            diag.clone(),
        )
        .unwrap();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        diag.observation.initialize(&owner).unwrap();
        let (tx, rx) = sparrow_io::observed::channel(capacity);
        let cancel = CancellationToken::new();
        let task =
            tokio::spawn(source.run_budgeted(tx, cancel.clone(), owner.clone(), max_row_bytes));
        Self {
            rx,
            owner,
            schema,
            diag,
            cancel,
            task,
        }
    }

    async fn take(&mut self, n: usize) -> Vec<Row> {
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while got.len() < n {
                got.push(self.rx.recv().await.expect("source closed"));
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {n} rows"));
        let mut b = RowBatchBuilder::new(
            self.schema.clone(),
            self.owner.clone(),
            CreditKind::Reservation,
            n.max(1),
            1 << 20,
        )
        .unwrap();
        for q in got {
            q.push_into(&mut b).unwrap();
        }
        b.finish().unwrap().rows().to_vec()
    }

    async fn nothing_for(&mut self, d: Duration) {
        assert!(
            tokio::time::timeout(d, self.rx.recv()).await.is_err(),
            "no further rows"
        );
    }

    fn tracking(&self) -> Option<i64> {
        let s = self.diag.snapshot();
        (s.postgres_source_tracking_set == 1).then_some(s.postgres_source_tracking_value)
    }

    async fn stop(self) -> (Result<()>, crate::diag::IoSnapshot) {
        self.cancel.cancel();
        let r = tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("source stops promptly")
            .unwrap();
        (r, self.diag.snapshot())
    }
}

fn ids(rows: &[Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r.values[0] {
            Scalar::Int64(v) => v,
            ref o => panic!("{o:?}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn real_source_tracks_progress_ties_and_oversize() {
    let Some(server) = Server::start().await else {
        return;
    };
    server
        .exec(
            "CREATE TABLE ev (id int8 PRIMARY KEY, grp int4 NOT NULL, body text, at timestamptz);\
             INSERT INTO ev SELECT g, CASE WHEN g <= 4 THEN 1 ELSE g END, 'r' || g, \
               timestamptz '2024-01-01 00:00:00+00' + g * interval '1 second' FROM generate_series(1, 7) g;\
             UPDATE ev SET body = repeat('x', 20000) WHERE id = 6",
        )
        .await;
    let s = schema(vec![
        field(1, "id", DataType::Int64, false),
        field(2, "grp", DataType::Int64, false),
        field(3, "body", DataType::Utf8, true),
        field(4, "at", DataType::TimestampMicrosUTC, true),
    ]);
    // Tracking by grp with fetch_rows 3: rows 1..4 share grp=1, so the first
    // page (1,1,1) cannot progress: the Source stops.
    let mut config = PgSourceConfig::new(
        server.tls_target(),
        "SELECT id, grp, body, at FROM ev",
        "grp",
        s.clone(),
    );
    config.fetch_rows = 3;
    config.poll_interval = Duration::from_millis(100);
    let h = SourceHarness::start(&server, config.clone(), 8 * 1024);
    let r = tokio::time::timeout(Duration::from_secs(10), h.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.unwrap_err().code, ErrorCode::BoundExceeded);
    // fetch_rows 5: page 1 = grp 1,1,1,1,5 (full) -> ties kept together:
    // admit only grp 1 rows; page 2 = 5, 6(oversize), 7.
    config.fetch_rows = 5;
    let mut h = SourceHarness::start(&server, config.clone(), 8 * 1024);
    let rows = h.take(6).await;
    // Order among equal tracking values is the server's (unspecified).
    let mut first = ids(&rows[..4]);
    first.sort_unstable();
    assert_eq!(first, [1, 2, 3, 4]);
    assert_eq!(ids(&rows[4..]), [5, 7]);
    let r1 = rows
        .iter()
        .find(|r| r.values[0] == Scalar::Int64(1))
        .unwrap();
    assert_eq!(
        r1.values[3],
        Scalar::TimestampMicrosUTC(1_704_067_201_000_000)
    );
    assert_eq!(r1.values[2], Scalar::utf8("r1"));
    until(Duration::from_secs(5), || {
        let t = h.tracking();
        Box::pin(async move { t == Some(7) })
    })
    .await;
    // New rows (and a late row with a smaller value, which is skipped:
    // live-only) are picked up by the next poll.
    server
        .exec("INSERT INTO ev VALUES (8, 8, 'late ok', NULL), (9, 3, 'late skipped', NULL)")
        .await;
    assert_eq!(ids(&h.take(1).await), [8]);
    h.nothing_for(Duration::from_millis(400)).await;
    // The server ends the session: the Source reconnects and continues.
    server
        .exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'app_pw'")
        .await;
    server
        .exec("INSERT INTO ev VALUES (10, 10, 'after', NULL)")
        .await;
    assert_eq!(ids(&h.take(1).await), [10]);
    let (r, snap) = h.stop().await;
    r.unwrap();
    assert_eq!(snap.postgres_source_dropped_oversize, 1);
    assert_eq!(snap.postgres_source_rows, 8);
    assert!(snap.postgres_source_connects >= 2);
    // start_after skips history (timestamp tracking, µs since 1970).
    let mut config = PgSourceConfig::new(
        server.plain_target("plain"),
        "SELECT id, grp, body, at FROM ev",
        "at",
        s,
    );
    config.start_after = Some(1_704_067_205_000_000);
    config.poll_interval = Duration::from_millis(100);
    config.fetch_rows = 20;
    let mut h = SourceHarness::start(&server, config, 64 * 1024);
    // at > 00:00:05 -> ids 6, 7 (id 8..10 have NULL at and are never read).
    assert_eq!(ids(&h.take(2).await), [6, 7]);
    h.nothing_for(Duration::from_millis(300)).await;
    assert_eq!(h.tracking(), Some(1_704_067_207_000_000));
    h.stop().await.0.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn real_source_refusals_and_stop_cancels_query() {
    let Some(server) = Server::start().await else {
        return;
    };
    server
        .exec("CREATE TABLE q (id int8, u uuid, f float8); INSERT INTO q VALUES (1, gen_random_uuid(), 'NaN')")
        .await;
    let s = schema(vec![field(1, "id", DataType::Int64, false)]);
    // Unmapped column type, wrong tracking type, syntax error: fail, no retry.
    // fetch_rows 20 x 64 KiB rows fits the compact reservation.
    let small = |query: &str, tracking: &str, sch: Schema| {
        let mut c = PgSourceConfig::new(server.plain_target("plain"), query, tracking, sch);
        c.fetch_rows = 20;
        c.poll_interval = Duration::from_millis(100);
        c
    };
    for (query, tracking, sch, code) in [
        (
            "SELECT id, u FROM q",
            "id",
            schema(vec![
                field(1, "id", DataType::Int64, false),
                field(2, "u", DataType::Utf8, true),
            ]),
            ErrorCode::InvalidSchema,
        ),
        (
            "SELECT id, f FROM q",
            "f",
            s.clone(),
            ErrorCode::InvalidSchema,
        ),
        (
            "SELEC id FROM q",
            "id",
            s.clone(),
            ErrorCode::InvalidArgument,
        ),
        (
            "SELECT id FROM q; DROP TABLE q",
            "id",
            s.clone(),
            ErrorCode::InvalidArgument,
        ),
    ] {
        let h = SourceHarness::start(&server, small(query, tracking, sch), 64 * 1024);
        let diag = h.diag.clone();
        let r = tokio::time::timeout(Duration::from_secs(10), h.task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.unwrap_err().code, code, "{query}");
        assert_eq!(diag.snapshot().postgres_source_connects, 1, "{query}");
    }
    assert_eq!(
        server.count("SELECT count(*) FROM q").await,
        1,
        "table survives"
    );
    // Non-finite float: dropped (fail_on_decode=false) or fatal (true).
    let fs = schema(vec![
        field(1, "id", DataType::Int64, false),
        field(2, "f", DataType::Float64, true),
    ]);
    let mut config = small("SELECT id, f FROM q", "id", fs);
    let mut h = SourceHarness::start(&server, config.clone(), 64 * 1024);
    until(Duration::from_secs(5), || {
        let t = h.tracking();
        Box::pin(async move { t == Some(1) })
    })
    .await;
    h.nothing_for(Duration::from_millis(100)).await;
    let (_, snap) = h.stop().await;
    assert_eq!(snap.postgres_source_dropped_bad, 1);
    config.fail_on_decode = true;
    let h = SourceHarness::start(&server, config, 64 * 1024);
    let r = tokio::time::timeout(Duration::from_secs(10), h.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.unwrap_err().code, ErrorCode::CodecViolation);
    // A query blocked on a lock: stop returns promptly and cancels it.
    let locker = server.admin().await;
    locker
        .client
        .batch_execute("BEGIN; LOCK TABLE q IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let h = SourceHarness::start(&server, small("SELECT id FROM q", "id", s), 64 * 1024);
    until(Duration::from_secs(5), || {
        Box::pin(async { server.busy("plain").await == 1 })
    })
    .await;
    let (r, _) = h.stop().await;
    r.unwrap();
    until(Duration::from_secs(5), || {
        Box::pin(async { server.busy("plain").await == 0 })
    })
    .await;
    locker.client.batch_execute("COMMIT").await.unwrap();
}

// ------------------------------------------------------------- lookup --

fn users_schema() -> Schema {
    schema(vec![
        field(1, "tenant", DataType::Utf8, false),
        field(2, "id", DataType::Int64, false),
        field(3, "name", DataType::Utf8, true),
        field(4, "score", DataType::Float64, true),
        field(5, "seen", DataType::TimestampMicrosUTC, true),
    ])
}

fn lookup_config(server: &Server, table: &str, keys: &[&str]) -> PgLookupConfig {
    PgLookupConfig {
        target: server.tls_target(),
        schema_name: "public".into(),
        table: table.into(),
        schema: users_schema(),
        keys: keys.iter().map(|k| k.to_string()).collect(),
        timeout: Duration::from_millis(1000),
        pool_size: 2,
    }
}

fn int(v: i64) -> Scalar {
    Scalar::Int64(v)
}

#[tokio::test(flavor = "multi_thread")]
async fn real_lookup_batches_misses_and_errors() {
    let Some(server) = Server::start().await else {
        return;
    };
    server
        .exec(
            "CREATE TABLE users (tenant text NOT NULL, id int4 NOT NULL, name varchar(20), score numeric, seen timestamptz, PRIMARY KEY (tenant, id));\
             INSERT INTO users VALUES ('a', 1, 'ann', 1.25, '2024-01-01 00:00:00+00'), ('a', 2, NULL, NULL, NULL), ('b', 1, 'bob', 2, NULL);\
             CREATE TABLE dupes (tenant text NOT NULL, id int8 NOT NULL, name text, score float8, seen timestamp);\
             INSERT INTO dupes VALUES ('a', 1, 'x', 1, NULL), ('a', 1, 'y', 2, NULL)",
        )
        .await;
    let policy = server.policy();
    let one = PgLookup::bind(
        lookup_config(&server, "users", &["id"]),
        &secrets(),
        &policy,
    );
    // id alone is not unique in users (a/1, b/1): a duplicate is an error.
    let one = one.unwrap();
    let e = one
        .lookup(vec![int(1)], CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::CodecViolation);
    let got = one
        .lookup(vec![int(2)], CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.values[1], int(2));
    assert_eq!(got.values[2], Scalar::Null);
    // Composite key, one statement for a batch with misses, duplicates and a
    // key outside int4 (a miss without asking the server).
    let two = PgLookup::bind(
        lookup_config(&server, "users", &["tenant", "id"]),
        &secrets(),
        &policy,
    )
    .unwrap();
    let key = |t: &str, i: i64| vec![Scalar::utf8(t), int(i)];
    let rows = two
        .lookup_batch(
            vec![
                key("a", 1),
                key("b", 1),
                key("z", 1),
                key("a", 1),
                key("a", 1 << 40),
            ],
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    let ann = rows[0].as_ref().unwrap();
    assert_eq!(
        ann.values,
        [
            Scalar::utf8("a"),
            int(1),
            Scalar::utf8("ann"),
            Scalar::Float64(1.25),
            Scalar::TimestampMicrosUTC(1_704_067_200_000_000)
        ]
    );
    assert_eq!(rows[1].as_ref().unwrap().values[2], Scalar::utf8("bob"));
    assert!(rows[2].is_none() && rows[4].is_none());
    assert_eq!(rows[3], rows[0]);
    // Non-unique table rows for a key: CodecViolation.
    let d = PgLookup::bind(
        lookup_config(&server, "dupes", &["tenant", "id"]),
        &secrets(),
        &policy,
    )
    .unwrap();
    assert_eq!(
        d.lookup(key("a", 1), CancellationToken::new())
            .await
            .unwrap_err()
            .code,
        ErrorCode::CodecViolation
    );
    // Missing table / column type that does not map: hard errors.
    let m = PgLookup::bind(lookup_config(&server, "nope", &["id"]), &secrets(), &policy).unwrap();
    assert_eq!(
        m.lookup(vec![int(1)], CancellationToken::new())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidArgument
    );
    // A query blocked on a lock: the timeout fails it (JobFailed) and the
    // server-side statement is cancelled.
    let locker = server.admin().await;
    locker
        .client
        .batch_execute("BEGIN; LOCK TABLE users IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let mut slow = lookup_config(&server, "users", &["tenant", "id"]);
    slow.timeout = Duration::from_millis(300);
    let slow = PgLookup::bind(slow, &secrets(), &policy).unwrap();
    let started = Instant::now();
    let e = slow
        .lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::JobFailed);
    assert!(started.elapsed() < Duration::from_secs(2));
    until(Duration::from_secs(5), || {
        Box::pin(async { server.busy("app_pw").await == 0 })
    })
    .await;
    locker.client.batch_execute("COMMIT").await.unwrap();
    // The pool recovers after the server closes idle sessions (one retry on
    // a fresh connection).
    two.lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    server
        .exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'app_pw'")
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    two.lookup(key("a", 1), CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
}

/// The same thin adapter the control plane uses (`PostgresProvider`).
struct Provider(PgLookup);

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

#[tokio::test(flavor = "multi_thread")]
async fn real_lookup_through_the_batched_operator() {
    let Some(server) = Server::start().await else {
        return;
    };
    server
        .exec(
            "CREATE TABLE users (tenant text NOT NULL, id int8 NOT NULL, name text, score float8, seen timestamp, PRIMARY KEY (tenant, id));\
             INSERT INTO users SELECT 'a', g, 'u' || g, g, NULL FROM generate_series(1, 6) g",
        )
        .await;
    let owner = MemoryOwner::new(ResourceBudget::performance());
    let lookup = PgLookup::bind(
        lookup_config(&server, "users", &["tenant", "id"]),
        &secrets(),
        &server.policy(),
    )
    .unwrap();
    let stream_schema = schema(vec![
        field(1, "s", DataType::Utf8, true),
        field(2, "d", DataType::Int64, true),
    ]);
    let mut op = ExternalLookupOperator::new(
        LookupSpec::static_table(
            "users",
            vec!["s".into(), "d".into()],
            vec!["tenant".into(), "id".into()],
            vec!["name".into()],
        ),
        ExternalLookupBinding {
            provider: Arc::new(Provider(lookup)),
            options: ExternalLookupOptions {
                max_inflight: 2,
                batch_keys: 4,
                ..Default::default()
            },
        },
        stream_schema.clone(),
        owner.clone(),
    )
    .unwrap();
    let keys: [(&str, i64); 7] = [
        ("a", 1),
        ("a", 2),
        ("x", 1),
        ("a", 3),
        ("a", 4),
        ("a", 5),
        ("a", 6),
    ];
    let mut b = RowBatchBuilder::new(
        Arc::new(stream_schema),
        owner.clone(),
        CreditKind::Reservation,
        8,
        1 << 20,
    )
    .unwrap();
    for (s, d) in keys {
        b.push(Row {
            values: vec![Scalar::utf8(s), Scalar::Int64(d)],
        })
        .unwrap();
    }
    let input = b.finish().unwrap();
    let out = op
        .on_batch_into(&input, CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    let names: Vec<Scalar> = out.rows().iter().map(|r| r.values[2].clone()).collect();
    assert_eq!(
        names,
        [
            Scalar::utf8("u1"),
            Scalar::utf8("u2"),
            Scalar::Null,
            Scalar::utf8("u3"),
            Scalar::utf8("u4"),
            Scalar::utf8("u5"),
            Scalar::utf8("u6")
        ]
    );
    let snap = op.diagnostics().snapshot();
    assert_eq!(
        (snap.requests, snap.hits, snap.misses),
        (2, 6, 1),
        "7 keys in 2 statements"
    );
    drop((out, input, op));
    assert_eq!(owner.usage().physical_bytes, 0, "scratch released");
}

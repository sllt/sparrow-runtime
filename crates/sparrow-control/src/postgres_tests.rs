//! PostgreSQL Source / Sink / Lookup through the real control path: spec
//! matrix (exclusive blocks, mixed fields, write modes, durable claims
//! refused for legacy and graph endpoints), lookup provider selection, IO
//! validation (target policy, sslmode, secrets, budgets, schema), builds
//! without the `postgres` feature, and (opt-in, `SPARROW_POSTGRES_BIN`)
//! Store -> Supervisor -> PostgreSQL Source -> Lookup(PostgreSQL) ->
//! PostgreSQL Sink against a real PostgreSQL 16 server.

use crate::PipelineSpec;
use serde_json::{json, Value};
use sparrow_model::ErrorCode;

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
type Mutation = Box<dyn Fn(&mut Value)>;

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
const STREAM: &str = r#"{"fields":[{"name":"id","type":"int64","nullable":false},{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"float64","nullable":true}]}"#;

fn conn(port: u16) -> Value {
    json!({"url": format!("postgresql://127.0.0.1:{port}/app"), "user": "plain", "sslmode": "disable"})
}

fn with(base: Value, extra: Value) -> Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

fn spec(port: u16) -> Value {
    json!({
        "version": 1,
        "stream": "readings",
        "external_lookups": {"limits": {
            "postgres": with(conn(port), json!({"table": "limits"})),
            "fields": [{"name":"device_id","type":"utf8","nullable":false},{"name":"threshold","type":"int64","nullable":true}],
            "keys": ["device_id"],
            "options": {"max_inflight": 1, "batch_keys": 7, "cache_ttl_ms": 0}
        }},
        "source": {"kind": "postgres", "inbox_capacity": 8, "postgres": with(conn(port), json!({
            "query": "SELECT id, device_id, v FROM readings",
            "tracking_column": "id",
            "poll_interval_ms": 100
        }))},
        "sink": {"kind": "postgres", "outbox_capacity": 8, "postgres": with(conn(port), json!({
            "table": "out", "mode": "upsert", "conflict_key": ["id"],
            "update_columns": ["device_id", "v", "threshold"]
        }))},
        "graph": {"version": 1, "pipeline_id": 1, "revision_id": 1, "nodes": [
            {"id": 1, "kind": "memory_source", "table": "readings", "out": [2]},
            {"id": 2, "kind": "lookup", "table": "limits", "on": [{"stream": "device_id", "table": "device_id"}], "keep": ["threshold"], "out": [3]},
            {"id": 3, "kind": "capture_sink"}
        ]},
        "recovery": "restart_fresh"
    })
}

#[cfg(not(feature = "postgres"))]
#[test]
fn postgres_requires_the_build_feature() {
    let base = spec(5432);
    assert_eq!(
        parse(&base).unwrap_err().code,
        ErrorCode::FeatureUnavailable
    );
    // Lookup only (File -> Log): refused too, not silently ignored.
    let mut v = base.clone();
    v["source"] = json!({"kind": "file", "path": "/tmp/unused.ndjson", "file_contract": "sealed"});
    v["sink"] = json!({"kind": "log"});
    assert_eq!(parse(&v).unwrap_err().code, ErrorCode::FeatureUnavailable);
    let caps = crate::capabilities_json();
    let pg = caps["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == "postgres")
        .unwrap()
        .clone();
    assert_eq!(pg["enabled_by_build"], false);
    assert!(!crate::capability::inventory()["combinations"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["source"] == "postgres" || c["sink"] == "postgres"));
}

#[cfg(feature = "postgres")]
mod enabled {
    use super::*;
    use crate::{request_start, Store, Supervisor};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn postgres_spec_matrix_rejects_mixed_fields_modes_and_durable_claims() {
        use ErrorCode::*;
        let base = spec(5432);
        let parsed = parse(&base).unwrap();
        let stored = serde_json::to_value(&parsed).unwrap();
        assert!(stored["sink"]["postgres"].get("chunk_rows").is_none());
        assert!(stored["external_lookups"]["limits"].get("url").is_none());
        assert_eq!(parse(&stored).unwrap(), parsed, "round trip");
        let cases: Vec<(&str, Mutation, ErrorCode)> = vec![
            (
                "missing sink block",
                Box::new(|v| {
                    v["sink"].as_object_mut().unwrap().remove("postgres");
                }),
                InvalidArgument,
            ),
            (
                "sink block on http",
                Box::new(|v| v["sink"]["kind"] = json!("http")),
                InvalidArgument,
            ),
            (
                "missing source block",
                Box::new(|v| {
                    v["source"].as_object_mut().unwrap().remove("postgres");
                }),
                InvalidArgument,
            ),
            (
                "source block on mqtt",
                Box::new(|v| v["source"]["kind"] = json!("mqtt")),
                InvalidArgument,
            ),
            (
                "unknown sink field",
                Box::new(|v| v["sink"]["postgres"]["copy"] = json!(true)),
                InvalidArgument,
            ),
            (
                "unknown source field",
                Box::new(|v| v["source"]["postgres"]["cdc"] = json!(true)),
                InvalidArgument,
            ),
            (
                "sink mixed url",
                Box::new(|v| v["sink"]["url"] = json!("https://localhost:1/")),
                InvalidArgument,
            ),
            (
                "sink mixed batch_rows",
                Box::new(|v| v["sink"]["batch_rows"] = json!(4)),
                InvalidArgument,
            ),
            (
                "sink mixed format",
                Box::new(|v| v["sink"]["format"] = json!("csv")),
                InvalidArgument,
            ),
            (
                "sink mixed action",
                Box::new(|v| v["sink"]["action"] = json!({"body": {"a": "$device_id"}})),
                InvalidArgument,
            ),
            (
                "source mixed tls",
                Box::new(|v| v["source"]["tls"] = json!(true)),
                InvalidArgument,
            ),
            (
                "source mixed format",
                Box::new(|v| v["source"]["format"] = json!("csv")),
                InvalidArgument,
            ),
            (
                "source mixed inbox_bytes",
                Box::new(|v| v["source"]["inbox_bytes"] = json!(1024)),
                InvalidArgument,
            ),
            (
                "source mixed password",
                Box::new(|v| v["source"]["password_secret"] = json!("pw")),
                InvalidArgument,
            ),
            (
                "aligned",
                Box::new(|v| {
                    v["recovery"] = json!("aligned");
                    v["checkpoint_dir"] = json!("/tmp/sparrow/pg");
                }),
                UnsupportedRestore,
            ),
            (
                "restore",
                Box::new(|v| v["restore"] = json!({"kind": "checkpoint", "snapshot_id": "1"})),
                UnsupportedRestore,
            ),
            (
                "checkpoint",
                Box::new(|v| v["checkpoint"] = json!({"interval_ms": 1000})),
                UnsupportedRestore,
            ),
        ];
        for (name, mutate, code) in cases {
            let mut v = base.clone();
            mutate(&mut v);
            let err = parse(&v).expect_err(name);
            assert_eq!(err.code, code, "{name}: {}", err.message);
        }
        let mut typed = parsed.clone();
        typed.checkpoint_dir = Some("/tmp/sparrow/pg".into());
        assert_eq!(typed.check_delivery().unwrap_err().code, UnsupportedRestore);

        // Write mode mapping.
        let config = |sink: Value| {
            let mut v = base.clone();
            v["sink"]["postgres"] = with(conn(5432), sink);
            crate::validate::postgres_sink_config(&parse(&v).unwrap().sink).map(|c| c.mode)
        };
        assert_eq!(
            config(json!({"table": "t", "mode": "insert"})).unwrap(),
            sparrow_connectors::postgres::PgWriteMode::Insert
        );
        assert_eq!(
            config(json!({"table": "t", "mode": "upsert", "conflict_key": ["id"], "update_columns": []})).unwrap(),
            sparrow_connectors::postgres::PgWriteMode::Upsert { conflict_key: vec!["id".into()], update_columns: vec![] }
        );
        for bad in [
            json!({"table": "t", "mode": "merge"}),
            json!({"table": "t", "mode": "insert", "conflict_key": ["id"]}),
            json!({"table": "t", "mode": "insert", "update_columns": ["v"]}),
            json!({"table": "t", "mode": "upsert", "conflict_key": ["id"]}),
            json!({"table": "t", "mode": "upsert", "update_columns": ["v"]}),
            json!({"table": "t", "mode": "insert", "columns": []}),
            json!({"table": "t", "mode": "insert", "sslmode": "prefer"}),
        ] {
            let e = config(bad.clone()).unwrap_err();
            assert!(
                matches!(e.code, InvalidArgument | PolicyDenied),
                "{bad}: {e}"
            );
        }
        let caps = crate::capabilities_json();
        for kind in ["postgres", "postgres_sink"] {
            let c = caps["connectors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["kind"] == kind)
                .unwrap()
                .clone();
            assert_eq!(c["enabled_by_build"], true);
            assert_eq!(
                (c["delivery"].as_str(), c["replay"].as_str()),
                (Some("live_best_effort"), Some("unsupported")),
                "{kind}"
            );
        }
        assert!(crate::capability::inventory()["combinations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["source"] == "postgres" && c["sink"] == "postgres"));
    }

    #[test]
    fn postgres_graph_endpoints_cannot_claim_durability() {
        let file = json!({"kind":"file", "path":"/tmp/unused.ndjson", "file_contract":"sealed", "inbox_capacity":8});
        let log = json!({"kind":"log", "outbox_capacity":8});
        let pg_sink = json!({"kind":"postgres", "postgres": with(conn(5432), json!({"table":"t","mode":"insert"}))});
        let pg_source = json!({"kind":"postgres", "postgres": with(conn(5432), json!({"query":"SELECT 1 AS id","tracking_column":"id"}))});
        for (source, sink) in [(file.clone(), pg_sink), (pg_source, log.clone())] {
            let mut value = json!({
                "version":1, "stream":"readings", "source":source.clone(), "sink":sink.clone(),
                "graph":{"version":1, "pipeline_id":910, "revision_id":1, "nodes":[
                    {"id":1, "kind":"memory_source", "table":"readings", "out":[2]},
                    {"id":2, "kind":"capture_sink", "name":"out"}
                ]},
                "graph_io":{"sources":{"1":source}, "sinks":{"2":sink}}
            });
            parse(&value).unwrap();
            value["recovery"] = json!("aligned");
            value["checkpoint_dir"] = json!("/tmp/sparrow/pg-graph");
            let err = parse(&value).unwrap_err();
            assert_eq!(err.code, ErrorCode::UnsupportedRestore, "{err}");
            let mut mixed = value.clone();
            mixed["recovery"] = json!("restart_fresh");
            mixed.as_object_mut().unwrap().remove("checkpoint_dir");
            for side in ["sources", "sinks"] {
                for (_, endpoint) in mixed["graph_io"][side].as_object_mut().unwrap() {
                    if endpoint["kind"] == "postgres" {
                        endpoint["topic"] = json!("x");
                    }
                }
            }
            assert_eq!(parse(&mixed).unwrap_err().code, ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn postgres_lookup_is_one_provider_among_three() {
        use ErrorCode::*;
        let base = spec(5432);
        let cases: Vec<(&str, Mutation, ErrorCode)> = vec![
            (
                "url and postgres",
                Box::new(|v| v["external_lookups"]["limits"]["url"] = json!("http://127.0.0.1:9/")),
                InvalidArgument,
            ),
            (
                "redis and postgres",
                Box::new(|v| {
                    v["external_lookups"]["limits"]["redis"] =
                        json!({"url": "redis://h", "key": "k:{device_id}"})
                }),
                InvalidArgument,
            ),
            (
                "header_secret",
                Box::new(|v| v["external_lookups"]["limits"]["header_secret"] = json!("pw")),
                InvalidArgument,
            ),
            (
                "unknown field",
                Box::new(|v| {
                    v["external_lookups"]["limits"]["postgres"]["query"] = json!("SELECT 1")
                }),
                InvalidArgument,
            ),
            (
                "batch_keys 65",
                Box::new(|v| v["external_lookups"]["limits"]["options"]["batch_keys"] = json!(65)),
                BoundExceeded,
            ),
            (
                "password without tls",
                Box::new(|v| {
                    v["external_lookups"]["limits"]["postgres"]["password_secret"] = json!("pw")
                }),
                PolicyDenied,
            ),
            (
                "sslmode require",
                Box::new(|v| {
                    v["external_lookups"]["limits"]["postgres"]["sslmode"] = json!("require")
                }),
                PolicyDenied,
            ),
            (
                "bad table",
                Box::new(|v| v["external_lookups"]["limits"]["postgres"]["table"] = json!("")),
                InvalidArgument,
            ),
        ];
        for (name, mutate, code) in cases {
            let mut v = base.clone();
            mutate(&mut v);
            let err = parse(&v).expect_err(name);
            assert_eq!(err.code, code, "{name}: {}", err.message);
        }
    }

    fn store() -> Arc<Store> {
        let store = Arc::new(Store::open_memory().unwrap());
        store.put_stream("readings", STREAM).unwrap();
        store.put_secret("pw", "hunter2").unwrap();
        store
    }

    #[test]
    fn postgres_validation_checks_target_secret_budget_and_schema() {
        use ErrorCode::*;
        let store = store();
        let secrets = sparrow_connectors::MapSecretResolver::new(
            [("pw".to_string(), "hunter2".to_string())]
                .into_iter()
                .collect(),
        );
        let allowed = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 5432);
        let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
            let spec = parse(value).map_err(|e| e.code)?;
            let plan = crate::bind_plan_with_store(&store, &spec, "p", 1).map_err(|e| e.code)?;
            let schema = crate::stream_to_schema(&store.get_stream(&spec.stream).unwrap())
                .map_err(|e| e.code)?;
            crate::validate::validate_io_with_plan(&spec, &schema, &plan, &secrets, policy, None)
                .map_err(|e| e.code)
        };
        let base = spec(5432);
        check(&base, &allowed).unwrap();
        assert_eq!(
            check(
                &base,
                &sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1)
            ),
            Err(PolicyDenied),
            "source, sink and lookup targets are policy-checked"
        );
        let tls = |v: &mut Value, side: &str| {
            v[side]["postgres"]["sslmode"] = json!("verify-full");
            v[side]["postgres"]["url"] = json!("postgresql://127.0.0.1:5432/app");
        };
        let cases: Vec<(&str, Mutation, ErrorCode)> = vec![
            (
                "sink password over disable",
                Box::new(|v| v["sink"]["postgres"]["password_secret"] = json!("pw")),
                PolicyDenied,
            ),
            (
                "source password over disable",
                Box::new(|v| v["source"]["postgres"]["password_secret"] = json!("pw")),
                PolicyDenied,
            ),
            (
                "sink sslmode require",
                Box::new(|v| v["sink"]["postgres"]["sslmode"] = json!("require")),
                PolicyDenied,
            ),
            (
                "source sslmode verify-ca",
                Box::new(|v| v["source"]["postgres"]["sslmode"] = json!("verify-ca")),
                PolicyDenied,
            ),
            (
                "sink missing secret",
                Box::new(move |v| {
                    tls(v, "sink");
                    v["sink"]["postgres"]["password_secret"] = json!("nope");
                }),
                SecretMissing,
            ),
            (
                "source missing secret",
                Box::new(move |v| {
                    tls(v, "source");
                    v["source"]["postgres"]["password_secret"] = json!("nope");
                }),
                SecretMissing,
            ),
            (
                "sink bad ca",
                Box::new(move |v| {
                    tls(v, "sink");
                    v["sink"]["postgres"]["ca_pem"] = json!("not a pem");
                }),
                InvalidArgument,
            ),
            (
                "userinfo in url",
                Box::new(|v| {
                    v["sink"]["postgres"]["url"] = json!("postgresql://u:p@127.0.0.1:5432/app")
                }),
                InvalidArgument,
            ),
            (
                "query string in url",
                Box::new(|v| {
                    v["source"]["postgres"]["url"] =
                        json!("postgresql://127.0.0.1:5432/app?sslmode=disable")
                }),
                InvalidArgument,
            ),
            (
                "chunk_bytes over budget",
                Box::new(|v| v["sink"]["postgres"]["chunk_bytes"] = json!(4 << 20)),
                BoundExceeded,
            ),
            (
                "chunk_bytes max",
                Box::new(|v| v["sink"]["postgres"]["chunk_bytes"] = json!(u64::MAX)),
                BoundExceeded,
            ),
            (
                "chunk_rows max",
                Box::new(|v| v["sink"]["postgres"]["chunk_rows"] = json!(u64::MAX)),
                BoundExceeded,
            ),
            (
                "retries",
                Box::new(|v| v["sink"]["postgres"]["max_retries"] = json!(21)),
                BoundExceeded,
            ),
            (
                "unknown sink column",
                Box::new(|v| {
                    v["sink"]["postgres"] = with(
                        conn(5432),
                        json!({"table": "out", "mode": "insert", "columns": ["nope"]}),
                    );
                }),
                InvalidSchema,
            ),
            (
                "conflict key not written",
                Box::new(|v| v["sink"]["postgres"]["columns"] = json!(["v"])),
                InvalidArgument,
            ),
            (
                "explicit fetch_rows over budget",
                Box::new(|v| v["source"]["postgres"]["fetch_rows"] = json!(1000)),
                BoundExceeded,
            ),
            (
                "fetch_rows 0",
                Box::new(|v| v["source"]["postgres"]["fetch_rows"] = json!(0)),
                BoundExceeded,
            ),
            (
                "fetch_rows max",
                Box::new(|v| v["source"]["postgres"]["fetch_rows"] = json!(u64::MAX)),
                BoundExceeded,
            ),
            (
                "source inbox_bytes max",
                Box::new(|v| v["source"]["postgres"]["inbox_bytes"] = json!(u64::MAX)),
                BoundExceeded,
            ),
            (
                "empty query",
                Box::new(|v| v["source"]["postgres"]["query"] = json!(" ")),
                InvalidArgument,
            ),
            (
                "bad tracking column",
                Box::new(|v| v["source"]["postgres"]["tracking_column"] = json!("")),
                InvalidArgument,
            ),
            (
                "poll interval",
                Box::new(|v| v["source"]["postgres"]["poll_interval_ms"] = json!(1)),
                BoundExceeded,
            ),
        ];
        for (name, mutate, code) in cases {
            let mut v = base.clone();
            mutate(&mut v);
            assert_eq!(check(&v, &allowed), Err(code), "{name}");
        }
    }

    // ------------------------------------------------- real PostgreSQL --

    struct Pg {
        child: Child,
        dir: PathBuf,
        bin: PathBuf,
        port: u16,
    }

    impl Drop for Pg {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    impl Pg {
        /// `None` (test skipped) unless `SPARROW_POSTGRES_BIN` is set.
        fn start() -> Option<Self> {
            let bin = PathBuf::from(std::env::var_os("SPARROW_POSTGRES_BIN")?);
            let version = Command::new(bin.join("postgres"))
                .arg("--version")
                .output()
                .unwrap();
            let version = String::from_utf8_lossy(&version.stdout).into_owned();
            assert!(
                version.contains(" 16."),
                "PostgreSQL 16.x expected, got {version}"
            );
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let dir =
                std::env::temp_dir().join(format!("sparrow-pg-ctl-{}-{port}", std::process::id()));
            let data = dir.join("data");
            std::fs::create_dir_all(&dir).unwrap();
            let out = Command::new(bin.join("initdb"))
                .arg("-D")
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
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            std::fs::write(
                data.join("postgresql.auto.conf"),
                format!(
                    "listen_addresses = '127.0.0.1'\nport = {port}\nunix_socket_directories = '{}'\nfsync = off\n",
                    dir.display()
                ),
            )
            .unwrap();
            let child = Command::new(bin.join("postgres"))
                .arg("-D")
                .arg(&data)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let pg = Self {
                child,
                dir,
                bin,
                port,
            };
            let started = Instant::now();
            while pg.try_sql("postgres", "SELECT 1").is_err() {
                assert!(
                    started.elapsed() < Duration::from_secs(20),
                    "postgres did not start"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            pg.sql("postgres", "CREATE DATABASE app");
            pg.sql("postgres", "CREATE ROLE plain LOGIN");
            Some(pg)
        }

        fn try_sql(&self, db: &str, sql: &str) -> Result<String, String> {
            let out = Command::new(self.bin.join("psql"))
                .args([
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &self.port.to_string(),
                    "-U",
                    "postgres",
                    "-d",
                    db,
                ])
                .args(["-v", "ON_ERROR_STOP=1", "-Atqc", sql])
                .output()
                .map_err(|e| e.to_string())?;
            if out.status.success() {
                Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                Err(String::from_utf8_lossy(&out.stderr).into_owned())
            }
        }

        fn sql(&self, db: &str, sql: &str) -> String {
            self.try_sql(db, sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
        }
    }

    async fn run_until(
        sup: &Arc<Supervisor>,
        store: &Store,
        name: &str,
        done: impl Fn() -> bool,
    ) -> crate::store::ActualState {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                sup.converge_once().await.unwrap();
                let actual = store.actual(name).unwrap();
                if done() || actual.status == "failed" {
                    break actual;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("pipeline progresses")
    }

    #[test]
    #[ignore = "requires SPARROW_POSTGRES_BIN (isolated PostgreSQL 16)"]
    fn real_postgres_source_lookup_and_upsert_sink_through_the_supervisor() {
        let pg = Pg::start().expect("SPARROW_POSTGRES_BIN is required");
        pg.sql(
            "app",
            "CREATE TABLE readings (id int8 PRIMARY KEY, device_id text NOT NULL, v float8);\
             CREATE TABLE limits (device_id text PRIMARY KEY, threshold int8);\
             CREATE TABLE out (id int8 PRIMARY KEY, device_id text, v float8, threshold int8);\
             CREATE TABLE locked (id int8 PRIMARY KEY, device_id text, v float8, threshold int8);\
             GRANT SELECT ON readings, limits TO plain; GRANT SELECT, INSERT, UPDATE ON out TO plain;\
             GRANT SELECT ON locked TO plain;\
             INSERT INTO limits VALUES ('d0', 10), ('d1', 11);\
             INSERT INTO readings SELECT g, 'd' || (g % 3), g * 0.5 FROM generate_series(1, 10) g",
        );
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let store = store();
            store.put_allow("127.0.0.1", pg.port).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            store.put_pipeline("pg", &parse(&spec(pg.port)).unwrap(), None).unwrap();
            request_start(&store, "pg", "test").unwrap();
            let count = || pg.sql("app", "SELECT count(*) FROM out").parse::<i64>().unwrap();
            let actual = run_until(&sup, &store, "pg", || count() == 10).await;
            assert_ne!(actual.status, "failed", "{actual:?}");
            assert_eq!(
                pg.sql("app", "SELECT string_agg(id || ':' || device_id || ':' || v || ':' || coalesce(threshold::text, '-'), ',' ORDER BY id) FROM out WHERE id <= 3"),
                "1:d1:0.5:11,2:d2:1:-,3:d0:1.5:10"
            );
            // New rows are picked up by the next poll; the lookup sees
            // table changes (cache_ttl_ms 0).
            pg.sql("app", "UPDATE limits SET threshold = 20 WHERE device_id = 'd2' ; INSERT INTO limits VALUES ('d2', 22) ON CONFLICT (device_id) DO UPDATE SET threshold = 22");
            pg.sql("app", "INSERT INTO readings VALUES (11, 'd2', 5.5), (12, 'd0', NULL)");
            let actual = run_until(&sup, &store, "pg", || count() == 12).await;
            assert_ne!(actual.status, "failed", "{actual:?}");
            assert_eq!(
                pg.sql("app", "SELECT string_agg(id || ':' || coalesce(v::text, 'null') || ':' || threshold, ',' ORDER BY id) FROM out WHERE id > 10"),
                "11:5.5:22,12:null:10"
            );
            let flow = sup.flow_snapshot("pg").unwrap().expect("running");
            assert_eq!((flow.source_kind, flow.sink_kind), ("postgres", "postgres"));
            let io = flow.diagnostics.snapshot();
            assert_eq!(io.postgres_source_rows, 12, "{io:?}");
            assert_eq!(io.postgres_source_tracking_value, 12);
            assert_eq!(io.postgres_sink_rows, 12, "{io:?}");
            assert_eq!(io.postgres_sink_fatal, 0);
            sup.stop_all().await;

            // A table the role may not write: the Sink stops the job.
            let mut locked = spec(pg.port);
            locked["sink"]["postgres"]["table"] = json!("locked");
            store.put_pipeline("pg2", &parse(&locked).unwrap(), None).unwrap();
            request_start(&store, "pg2", "test").unwrap();
            let actual = run_until(&sup, &store, "pg2", || false).await;
            assert_eq!(actual.status, "failed");
            let error = actual.last_error.unwrap_or_default();
            assert!(error.contains("PostgreSQL Sink stopped the job"), "{error}");
            assert_eq!(pg.sql("app", "SELECT count(*) FROM locked"), "0");
            sup.stop_all().await;
        });
        let _ = Path::new(&pg.dir);
    }
}

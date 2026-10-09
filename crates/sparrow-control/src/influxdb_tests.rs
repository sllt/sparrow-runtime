//! InfluxDB Sink through the real control path: spec matrix (exclusive
//! block, mixed fields, durable claims refused for legacy and graph sinks),
//! validation (target policy, secret, budget, schema mapping), and
//! Store -> Supervisor -> File -> SQL -> InfluxDB Sink against an in-process
//! HTTPS mock of `/api/v2/write`, including the fatal 401 path.

use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[path = "../../sparrow-connectors/src/mqtt/tls_fixture.rs"]
mod tls_fixture;

const SCHEMA: &str = r#"{"fields":[
  {"name":"device_id","type":"utf8","nullable":false},
  {"name":"temperature","type":"float64","nullable":true},
  {"name":"ts","type":"timestamp","nullable":false}
]}"#;

type Mutation = Box<dyn Fn(&mut Value)>;

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

fn ca() -> String {
    String::from_utf8(tls_fixture::CA.to_vec()).unwrap()
}

fn spec(path: &std::path::Path, port: u16) -> Value {
    json!({
        "version": 1,
        "stream": "telemetry",
        "sql": "SELECT device_id, temperature, ts FROM telemetry WHERE temperature > 0",
        "source": {"kind": "file", "path": path, "file_contract": "sealed", "inbox_capacity": 8},
        "sink": {"kind": "influxdb", "outbox_capacity": 8, "influxdb": {
            "url": format!("https://localhost:{port}"),
            "org": "acme", "bucket": "iot", "token_secret": "influx",
            "ca_pem": ca(),
            "measurement": "telemetry",
            "tags": ["device_id"],
            "time_column": "ts", "precision": "us",
            "batch_rows": 16, "flush_interval_ms": 20
        }},
        "recovery": "restart_fresh"
    })
}

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn input(n: usize) -> (Dir, PathBuf) {
    let dir = sparrow_connectors::ensure_default_data_root().join(format!(
        "influxdb-control-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("input.ndjson");
    let mut body = String::new();
    for i in 0..n {
        let t = if i % 2 == 0 { 0.5 + i as f64 } else { -1.0 };
        body.push_str(
            &json!({"device_id": format!("d {i}"), "temperature": t, "ts": 1_000_000 + i as i64})
                .to_string(),
        );
        body.push('\n');
    }
    std::fs::write(&path, body).unwrap();
    (Dir(dir), path)
}

fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("telemetry", SCHEMA).unwrap();
    store.put_secret("influx", "tok-123").unwrap();
    store
}

/// HTTPS `/api/v2/write` mock: records (request line, authorization, body)
/// and answers every request with `status`.
struct Mock {
    port: u16,
    seen: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl Mock {
    async fn start(status: u16) -> Self {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from_pem_slice(tls_fixture::CERT).unwrap()],
                PrivateKeyDer::from_pem_slice(tls_fixture::KEY).unwrap(),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let log = log.clone();
                tokio::spawn(async move {
                    let Ok(mut s) = acceptor.accept(socket).await else {
                        return;
                    };
                    let mut buf = Vec::new();
                    loop {
                        let head_end = loop {
                            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break i;
                            }
                            let mut chunk = [0u8; 4096];
                            match s.read(&mut chunk).await {
                                Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                                _ => return,
                            }
                        };
                        let head = String::from_utf8(buf[..head_end].to_vec()).unwrap();
                        let header = |name: &str| {
                            head.split("\r\n")
                                .filter_map(|l| l.split_once(':'))
                                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                                .map(|(_, v)| v.trim().to_string())
                                .unwrap_or_default()
                        };
                        let len: usize = header("content-length").parse().unwrap_or(0);
                        buf.drain(..head_end + 4);
                        while buf.len() < len {
                            let mut chunk = [0u8; 4096];
                            match s.read(&mut chunk).await {
                                Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                                _ => return,
                            }
                        }
                        let body = String::from_utf8(buf.drain(..len).collect()).unwrap();
                        let line = head.split("\r\n").next().unwrap().to_string();
                        log.lock()
                            .unwrap()
                            .push((line, header("authorization"), body));
                        let reply = format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\n\r\n");
                        if s.write_all(reply.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self { port, seen }
    }

    fn lines(&self) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(_, _, body)| body.lines().map(str::to_string).collect::<Vec<_>>())
            .collect()
    }
}

#[test]
fn influxdb_spec_matrix_rejects_mixed_fields_and_durable_claims() {
    use sparrow_model::ErrorCode::*;
    let base = spec(std::path::Path::new("/tmp/unused.ndjson"), 8086);
    let parsed = parse(&base).unwrap();
    let stored = serde_json::to_value(&parsed).unwrap();
    assert!(stored["sink"]["influxdb"].get("fields").is_none());
    assert!(stored["sink"]["influxdb"]
        .get("measurement_column")
        .is_none());
    assert_eq!(parse(&stored).unwrap(), parsed, "round trip");
    let cases: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "missing block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("influxdb");
            }),
            InvalidArgument,
        ),
        (
            "block on http",
            Box::new(|v| v["sink"]["kind"] = json!("http")),
            InvalidArgument,
        ),
        (
            "unknown field",
            Box::new(|v| v["sink"]["influxdb"]["retention"] = json!("1h")),
            InvalidArgument,
        ),
        (
            "mixed url",
            Box::new(|v| v["sink"]["url"] = json!("https://localhost:1/")),
            InvalidArgument,
        ),
        (
            "mixed batch_rows",
            Box::new(|v| v["sink"]["batch_rows"] = json!(4)),
            InvalidArgument,
        ),
        (
            "mixed action",
            Box::new(|v| v["sink"]["action"] = json!({"body": {"a": "$device_id"}})),
            InvalidArgument,
        ),
        (
            "mixed databus",
            Box::new(|v| v["sink"]["databus"] = json!({"topic": "x"})),
            InvalidArgument,
        ),
        (
            "aligned",
            Box::new(|v| {
                v["recovery"] = json!("aligned");
                v["checkpoint_dir"] = json!("/tmp/sparrow/i");
            }),
            UnsupportedRestore,
        ),
        (
            "restore",
            Box::new(|v| v["restore"] = json!({"kind": "checkpoint", "snapshot_id": "1"})),
            UnsupportedRestore,
        ),
        (
            "checkpoint_dir",
            Box::new(|v| v["checkpoint_dir"] = json!("/tmp/sparrow/i")),
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
    // Typed callers hit the same gate in check_delivery.
    let mut typed = parsed.clone();
    typed.recovery = "aligned".into();
    assert_eq!(typed.check_delivery().unwrap_err().code, UnsupportedRestore);
    // Connector config mapping errors.
    let config = |m: Mutation| {
        let mut v = base.clone();
        m(&mut v);
        crate::validate::influxdb_sink_config(&parse(&v).unwrap().sink).map(|_| ())
    };
    assert_eq!(
        config(Box::new(
            |v| v["sink"]["influxdb"]["measurement_column"] = json!("device_id")
        ))
        .unwrap_err()
        .code,
        InvalidArgument
    );
    assert_eq!(
        config(Box::new(|v| {
            v["sink"]["influxdb"]
                .as_object_mut()
                .unwrap()
                .remove("measurement");
        }))
        .unwrap_err()
        .code,
        InvalidArgument
    );
    assert_eq!(
        config(Box::new(|v| {
            v["sink"]["influxdb"]
                .as_object_mut()
                .unwrap()
                .remove("time_column");
        }))
        .unwrap_err()
        .code,
        InvalidArgument,
        "precision without time_column"
    );
    assert_eq!(
        config(Box::new(|v| v["sink"]["influxdb"]["precision"] = json!("m")))
            .unwrap_err()
            .code,
        InvalidArgument
    );
    let caps = crate::capabilities_json();
    let c = caps["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == "influxdb_sink")
        .unwrap()
        .clone();
    assert_eq!(c["delivery"], "live_best_effort");
    assert_eq!(c["replay"], "unsupported");
}

#[test]
fn influxdb_graph_sink_cannot_claim_durability() {
    use sparrow_model::ErrorCode;
    let (_dir, path) = input(1);
    let file = json!({"kind":"file", "path":path, "file_contract":"sealed", "inbox_capacity":8});
    let log = json!({"kind":"log", "outbox_capacity":8});
    let influx = spec(&path, 8086)["sink"].clone();
    let mut value = json!({
        "version":1, "stream":"telemetry", "source":file, "sink":log,
        "graph":{"version":1, "pipeline_id":907, "revision_id":1, "nodes":[
            {"id":1, "kind":"memory_source", "table":"telemetry", "out":[2]},
            {"id":2, "kind":"branch", "out":[3,4]},
            {"id":3, "kind":"capture_sink", "name":"log"},
            {"id":4, "kind":"capture_sink", "name":"influx"}
        ]},
        "graph_io":{"sources":{"1":file}, "sinks":{"3":log, "4":influx}}
    });
    parse(&value).unwrap();
    value["recovery"] = json!("aligned");
    value["checkpoint_dir"] = json!("/tmp/sparrow/influx-graph");
    let err = parse(&value).unwrap_err();
    assert_eq!(err.code, ErrorCode::UnsupportedRestore, "{err}");
    assert!(err.message.contains("InfluxDB"), "{err}");
    let mut mixed = value.clone();
    mixed["recovery"] = json!("restart_fresh");
    mixed.as_object_mut().unwrap().remove("checkpoint_dir");
    mixed["graph_io"]["sinks"]["4"]["url"] = json!("https://localhost:1/");
    assert_eq!(parse(&mixed).unwrap_err().code, ErrorCode::InvalidArgument);
}

#[test]
fn influxdb_validation_checks_target_secret_budget_and_schema() {
    use sparrow_model::ErrorCode::*;
    let store = store();
    let secrets = sparrow_connectors::MapSecretResolver::new(
        [("influx".to_string(), "tok".to_string())]
            .into_iter()
            .collect(),
    );
    let allowed = sparrow_connectors::TargetPolicy::allow("localhost", 8086);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        let spec = parse(value).map_err(|e| e.code)?;
        let plan = crate::bind_plan_with_store(&store, &spec, "p", 1).map_err(|e| e.code)?;
        let schema = crate::stream_to_schema(&store.get_stream(&spec.stream).unwrap())
            .map_err(|e| e.code)?;
        crate::validate::validate_io_with_plan(&spec, &schema, &plan, &secrets, policy, None)
            .map_err(|e| e.code)
    };
    let (_dir, path) = input(1);
    let base = spec(&path, 8086);
    check(&base, &allowed).unwrap();
    assert_eq!(
        check(
            &base,
            &sparrow_connectors::TargetPolicy::allow("localhost", 1)
        ),
        Err(PolicyDenied)
    );
    let cases: Vec<(&str, Mutation, sparrow_model::ErrorCode)> = vec![
        (
            "plain http",
            Box::new(|v| v["sink"]["influxdb"]["url"] = json!("http://localhost:8086")),
            PolicyDenied,
        ),
        (
            "query in url",
            Box::new(|v| v["sink"]["influxdb"]["url"] = json!("https://localhost:8086/?org=x")),
            InvalidArgument,
        ),
        (
            "missing secret",
            Box::new(|v| v["sink"]["influxdb"]["token_secret"] = json!("nope")),
            SecretMissing,
        ),
        (
            "batch_bytes",
            Box::new(|v| v["sink"]["influxdb"]["batch_bytes"] = json!(512)),
            BoundExceeded,
        ),
        (
            "batch_bytes max",
            Box::new(|v| v["sink"]["influxdb"]["batch_bytes"] = json!(u64::MAX)),
            BoundExceeded,
        ),
        (
            "gzip budget",
            Box::new(|v| {
                v["sink"]["influxdb"]["batch_bytes"] = json!(1 << 20);
                v["sink"]["influxdb"]["gzip"] = json!(true);
            }),
            BoundExceeded,
        ),
        (
            "retries",
            Box::new(|v| v["sink"]["influxdb"]["max_retries"] = json!(21)),
            BoundExceeded,
        ),
        (
            "bad ca",
            Box::new(|v| v["sink"]["influxdb"]["ca_pem"] = json!("not a pem")),
            InvalidArgument,
        ),
        (
            "unknown tag column",
            Box::new(|v| v["sink"]["influxdb"]["tags"] = json!(["nope"])),
            InvalidSchema,
        ),
        (
            "float tag",
            Box::new(|v| v["sink"]["influxdb"]["tags"] = json!(["temperature"])),
            InvalidSchema,
        ),
        (
            "timestamp field",
            Box::new(|v| {
                v["sink"]["influxdb"]
                    .as_object_mut()
                    .unwrap()
                    .remove("time_column");
                v["sink"]["influxdb"]
                    .as_object_mut()
                    .unwrap()
                    .remove("precision");
            }),
            InvalidSchema,
        ),
        (
            "time column not a timestamp",
            Box::new(|v| v["sink"]["influxdb"]["time_column"] = json!("temperature")),
            InvalidSchema,
        ),
        (
            "reserved name",
            Box::new(|v| v["sink"]["influxdb"]["measurement"] = json!("_m")),
            InvalidArgument,
        ),
    ];
    for (name, mutate, code) in cases {
        let mut v = base.clone();
        mutate(&mut v);
        assert_eq!(check(&v, &allowed), Err(code), "{name}");
    }
}

#[test]
fn influxdb_sink_end_to_end_through_the_supervisor() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        const N: usize = 40;
        let (_dir, path) = input(N);
        let mock = Mock::start(204).await;
        let store = store();
        store.put_allow("localhost", mock.port).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        store
            .put_pipeline("influx", &parse(&spec(&path, mock.port)).unwrap(), None)
            .unwrap();
        request_start(&store, "influx", "test").unwrap();
        let mut diag = None;
        tokio::time::timeout(Duration::from_secs(10), async {
            while mock.lines().len() < N / 2 {
                sup.converge_once().await.unwrap();
                if diag.is_none() {
                    diag = sup.flow_snapshot("influx").unwrap().map(|f| f.diagnostics);
                }
                let actual = store.actual("influx").unwrap();
                assert_ne!(actual.status, "failed", "{actual:?}");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("rows written");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let expected: Vec<String> = (0..N)
            .step_by(2)
            .map(|i| {
                format!(
                    "telemetry,device_id=d\\ {i} temperature={:?} {}",
                    0.5 + i as f64,
                    1_000_000 + i
                )
            })
            .collect();
        assert_eq!(
            mock.lines(),
            expected,
            "exact line protocol, order, no duplicates"
        );
        let seen = mock.seen.lock().unwrap().clone();
        assert!(seen.iter().all(|(line, auth, _)| line
            == "POST /api/v2/write?org=acme&bucket=iot&precision=us HTTP/1.1"
            && auth == "Token tok-123"));
        assert!(
            seen.iter().all(|(_, _, body)| body.lines().count() <= 16),
            "batch_rows"
        );
        let io = diag.expect("observed").snapshot();
        assert_eq!(io.influxdb_sink_rows_written, (N / 2) as u64, "{io:?}");
        assert_eq!(io.influxdb_sink_fatal, 0);
        sup.stop_all().await;
    });
}

#[test]
fn influxdb_unauthorized_fails_the_job() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (_dir, path) = input(4);
        let mock = Mock::start(401).await;
        let store = store();
        store.put_allow("localhost", mock.port).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        store
            .put_pipeline("influx", &parse(&spec(&path, mock.port)).unwrap(), None)
            .unwrap();
        request_start(&store, "influx", "test").unwrap();
        let actual = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                sup.converge_once().await.unwrap();
                let actual = store.actual("influx").unwrap();
                if actual.status == "failed" {
                    break actual;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("job fails");
        let error = actual.last_error.unwrap_or_default();
        assert!(error.contains("InfluxDB Sink stopped the job"), "{error}");
        assert_eq!(mock.seen.lock().unwrap().len(), 1, "no retry after 401");
        sup.stop_all().await;
    });
}

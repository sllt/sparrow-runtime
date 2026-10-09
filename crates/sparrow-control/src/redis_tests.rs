//! Redis Sink and Redis external Lookup through the real control path: spec
//! matrix (exclusive block, mixed fields, command options, durable claims
//! refused for legacy and graph sinks), lookup provider selection, IO
//! validation (target policy, TLS-only credentials, secrets, budget, schema)
//! and Store -> Supervisor -> File -> Lookup(Redis) -> Redis Sink against an
//! in-process RESP2 mock, including the fatal NOAUTH path.

use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use sparrow_model::ErrorCode;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Mutation = Box<dyn Fn(&mut Value)>;

fn parse(value: &Value) -> sparrow_model::Result<PipelineSpec> {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap())
}

const FIELDS: &str = r#"[{"name":"device_id","type":"utf8","nullable":false},{"name":"threshold","type":"int64","nullable":true}]"#;

fn spec(path: &std::path::Path, port: u16) -> Value {
    json!({
        "version": 1,
        "stream": "sensors",
        "external_lookups": {"limits": {
            "redis": {"url": format!("redis://127.0.0.1:{port}"), "key": "lim:{device_id}"},
            "fields": serde_json::from_str::<Value>(FIELDS).unwrap(),
            "keys": ["device_id"],
            "options": {"max_inflight": 1, "batch_keys": 4, "cache_ttl_ms": 0}
        }},
        "source": {"kind": "file", "path": path, "file_contract": "sealed", "inbox_capacity": 8},
        "sink": {"kind": "redis", "outbox_capacity": 8, "redis": {
            "url": format!("redis://127.0.0.1:{port}/2"),
            "command": "hset", "key": "out:{device_id}", "fields": ["threshold"],
            "pipeline_rows": 4, "flush_interval_ms": 5
        }},
        "graph": {"version": 1, "pipeline_id": 1, "revision_id": 1, "nodes": [
            {"id": 1, "kind": "memory_source", "table": "sensors", "out": [2]},
            {"id": 2, "kind": "lookup", "table": "limits", "on": [{"stream": "device_id", "table": "device_id"}], "keep": ["threshold"], "out": [3]},
            {"id": 3, "kind": "capture_sink"}
        ]},
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
        "redis-control-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("input.ndjson");
    let body: String = (0..n)
        .map(|i| format!("{}\n", json!({"device_id": format!("d{i}")})))
        .collect();
    std::fs::write(&path, body).unwrap();
    (Dir(dir), path)
}

fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store
        .put_stream(
            "sensors",
            r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false}]}"#,
        )
        .unwrap();
    store.put_secret("pw", "hunter2").unwrap();
    store
}

/// Plain-TCP RESP2 mock: HMGET lim:dN answers threshold N for even N (odd
/// keys miss), SELECT answers +OK, HSET answers `hset` verbatim.
struct Mock {
    port: u16,
    log: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Mock {
    async fn start(hset: &'static [u8]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let log = Arc::new(Mutex::new(Vec::new()));
        let l = log.clone();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let log = l.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    loop {
                        while let Some(args) = take(&mut buf) {
                            log.lock().unwrap().push(args.clone());
                            let reply: Vec<u8> = match args[0].as_str() {
                                "SELECT" => b"+OK\r\n".to_vec(),
                                "HSET" => hset.to_vec(),
                                "HMGET" => {
                                    let n: i64 =
                                        args[1].trim_start_matches("lim:d").parse().unwrap();
                                    if n % 2 == 0 {
                                        let v = n.to_string();
                                        format!("*1\r\n${}\r\n{v}\r\n", v.len()).into_bytes()
                                    } else {
                                        b"*1\r\n$-1\r\n".to_vec()
                                    }
                                }
                                _ => b"-ERR unknown\r\n".to_vec(),
                            };
                            if s.write_all(&reply).await.is_err() {
                                return;
                            }
                        }
                        let mut chunk = [0u8; 4096];
                        match s.read(&mut chunk).await {
                            Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                            _ => return,
                        }
                    }
                });
            }
        });
        Self { port, log }
    }

    fn named(&self, name: &str) -> Vec<Vec<String>> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a[0] == name)
            .cloned()
            .collect()
    }
}

fn take(buf: &mut Vec<u8>) -> Option<Vec<String>> {
    fn line(buf: &[u8], at: usize) -> Option<(usize, usize)> {
        let end = buf.get(at..)?.windows(2).position(|w| w == b"\r\n")? + at;
        let n = std::str::from_utf8(&buf[at + 1..end]).ok()?.parse().ok()?;
        Some((n, end + 2))
    }
    let (n, mut at) = line(buf, 0)?;
    let mut args = Vec::new();
    for _ in 0..n {
        let (len, start) = line(buf, at)?;
        if buf.len() < start + len + 2 {
            return None;
        }
        args.push(String::from_utf8_lossy(&buf[start..start + len]).to_string());
        at = start + len + 2;
    }
    buf.drain(..at);
    Some(args)
}

#[test]
fn redis_sink_spec_matrix_rejects_mixed_fields_options_and_durable_claims() {
    use ErrorCode::*;
    let base = spec(std::path::Path::new("/tmp/unused.ndjson"), 6379);
    let parsed = parse(&base).unwrap();
    let stored = serde_json::to_value(&parsed).unwrap();
    assert!(stored["sink"]["redis"].get("approximate").is_none());
    assert!(stored["external_lookups"]["limits"].get("url").is_none());
    assert_eq!(parse(&stored).unwrap(), parsed, "round trip");
    let cases: Vec<(&str, Mutation, ErrorCode)> = vec![
        (
            "missing block",
            Box::new(|v| {
                v["sink"].as_object_mut().unwrap().remove("redis");
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
            Box::new(|v| v["sink"]["redis"]["cluster"] = json!(true)),
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
            "mixed format",
            Box::new(|v| v["sink"]["format"] = json!("csv")),
            InvalidArgument,
        ),
        (
            "mixed action",
            Box::new(|v| v["sink"]["action"] = json!({"body": {"a": "$device_id"}})),
            InvalidArgument,
        ),
        (
            "aligned",
            Box::new(|v| {
                v["recovery"] = json!("aligned");
                v["checkpoint_dir"] = json!("/tmp/sparrow/r");
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
    typed.checkpoint_dir = Some("/tmp/sparrow/r".into());
    assert_eq!(typed.check_delivery().unwrap_err().code, UnsupportedRestore);

    // Command option mapping.
    let config = |sink: Value| {
        let mut v = base.clone();
        v["sink"]["redis"] = sink;
        crate::validate::redis_sink_config(&parse(&v).unwrap().sink).map(|c| c.command)
    };
    let url = "redis://h";
    for ok in [
        json!({"url": url, "command": "set", "key": "k:{device_id}", "ttl_ms": 1000}),
        json!({"url": url, "command": "set", "key": "k", "value_column": "threshold"}),
        json!({"url": url, "command": "hset", "key": "k", "field": "{device_id}"}),
        json!({"url": url, "command": "xadd", "key": "s", "fields": ["threshold"], "maxlen": 10, "approximate": true}),
        json!({"url": url, "command": "publish", "channel": "c.{device_id}"}),
        json!({"url": url, "command": "lpush", "key": "q"}),
        json!({"url": url, "command": "rpush", "key": "q", "value_column": "device_id"}),
    ] {
        config(ok.clone()).unwrap_or_else(|e| panic!("{ok}: {e}"));
    }
    for bad in [
        json!({"url": url, "command": "del", "key": "k"}),
        json!({"url": url, "command": "set"}),
        json!({"url": url, "command": "set", "key": "k", "channel": "c"}),
        json!({"url": url, "command": "publish", "key": "k"}),
        json!({"url": url, "command": "xadd", "key": "s", "fields": ["threshold"], "ttl_ms": 5}),
        json!({"url": url, "command": "xadd", "key": "s"}),
        json!({"url": url, "command": "xadd", "key": "s", "fields": ["threshold"], "value_column": "threshold"}),
        json!({"url": url, "command": "set", "key": "k", "approximate": true}),
        json!({"url": url, "command": "set", "key": "k", "maxlen": 5}),
        json!({"url": url, "command": "hset", "key": "k"}),
        json!({"url": url, "command": "hset", "key": "k", "fields": ["threshold"], "field": "f"}),
        json!({"url": url, "command": "hset", "key": "k", "fields": ["threshold"], "value_column": "threshold"}),
        json!({"url": url, "command": "set", "key": "{unclosed"}),
        json!({"url": url, "command": "set", "key": "{a}{b}"}),
    ] {
        assert_eq!(
            config(bad.clone()).unwrap_err().code,
            InvalidArgument,
            "{bad}"
        );
    }
    let caps = crate::capabilities_json();
    let c = caps["connectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["kind"] == "redis_sink")
        .unwrap()
        .clone();
    assert_eq!(
        (c["delivery"].as_str(), c["replay"].as_str()),
        (Some("live_best_effort"), Some("unsupported"))
    );
}

#[test]
fn redis_graph_sink_cannot_claim_durability() {
    let (_dir, path) = input(1);
    let file = json!({"kind":"file", "path":path, "file_contract":"sealed", "inbox_capacity":8});
    let log = json!({"kind":"log", "outbox_capacity":8});
    let redis = json!({"kind":"redis", "redis":{"url":"redis://127.0.0.1:6379", "command":"rpush", "key":"q"}});
    let mut value = json!({
        "version":1, "stream":"sensors", "source":file, "sink":log,
        "graph":{"version":1, "pipeline_id":907, "revision_id":1, "nodes":[
            {"id":1, "kind":"memory_source", "table":"sensors", "out":[2]},
            {"id":2, "kind":"branch", "out":[3,4]},
            {"id":3, "kind":"capture_sink", "name":"log"},
            {"id":4, "kind":"capture_sink", "name":"redis"}
        ]},
        "graph_io":{"sources":{"1":file}, "sinks":{"3":log, "4":redis}}
    });
    parse(&value).unwrap();
    value["recovery"] = json!("aligned");
    value["checkpoint_dir"] = json!("/tmp/sparrow/redis-graph");
    let err = parse(&value).unwrap_err();
    assert_eq!(err.code, ErrorCode::UnsupportedRestore, "{err}");
    assert!(err.message.contains("Redis"), "{err}");
    let mut mixed = value.clone();
    mixed["recovery"] = json!("restart_fresh");
    mixed.as_object_mut().unwrap().remove("checkpoint_dir");
    mixed["graph_io"]["sinks"]["4"]["topic"] = json!("x");
    assert_eq!(parse(&mixed).unwrap_err().code, ErrorCode::InvalidArgument);
}

#[test]
fn external_lookup_needs_exactly_one_provider_and_http_keeps_its_shape() {
    use ErrorCode::*;
    let base = spec(std::path::Path::new("/tmp/unused.ndjson"), 6379);
    let http = json!({"url":"http://127.0.0.1:9/lookup","fields":serde_json::from_str::<Value>(FIELDS).unwrap(),"keys":["device_id"]});
    // HTTP bindings serialize exactly as before (no new keys).
    let mut v = base.clone();
    v["external_lookups"]["limits"] = http.clone();
    let parsed = parse(&v).unwrap();
    assert_eq!(
        serde_json::to_value(&parsed.external_lookups["limits"]).unwrap(),
        json!({"url":"http://127.0.0.1:9/lookup","fields":serde_json::from_str::<Value>(FIELDS).unwrap(),"keys":["device_id"],
            "options":{"max_inflight":4,"timeout_ms":1000,"cache_ttl_ms":1000,"cache_bytes":65536,"on_error":"fail"}})
    );
    let cases: Vec<(&str, Mutation, ErrorCode)> = vec![
        (
            "both",
            Box::new(|v| v["external_lookups"]["limits"]["url"] = json!("http://127.0.0.1:9/")),
            InvalidArgument,
        ),
        (
            "neither",
            Box::new(|v| {
                v["external_lookups"]["limits"]
                    .as_object_mut()
                    .unwrap()
                    .remove("redis");
            }),
            InvalidArgument,
        ),
        (
            "header_secret on redis",
            Box::new(|v| v["external_lookups"]["limits"]["header_secret"] = json!("pw")),
            InvalidArgument,
        ),
        (
            "bad format",
            Box::new(|v| v["external_lookups"]["limits"]["redis"]["format"] = json!("string")),
            InvalidArgument,
        ),
        (
            "key misses a key column",
            Box::new(|v| v["external_lookups"]["limits"]["redis"]["key"] = json!("lim")),
            InvalidArgument,
        ),
        (
            "key uses a value column",
            Box::new(|v| {
                v["external_lookups"]["limits"]["redis"]["key"] =
                    json!("lim:{device_id}:{threshold}")
            }),
            InvalidArgument,
        ),
        (
            "batch_keys 65",
            Box::new(|v| v["external_lookups"]["limits"]["options"]["batch_keys"] = json!(65)),
            BoundExceeded,
        ),
        (
            "unknown redis field",
            Box::new(|v| v["external_lookups"]["limits"]["redis"]["db"] = json!(1)),
            InvalidArgument,
        ),
        (
            "password without tls",
            Box::new(|v| v["external_lookups"]["limits"]["redis"]["password_secret"] = json!("pw")),
            PolicyDenied,
        ),
    ];
    for (name, mutate, code) in cases {
        let mut v = base.clone();
        mutate(&mut v);
        let err = parse(&v).expect_err(name);
        assert_eq!(err.code, code, "{name}: {}", err.message);
    }
    // HTTP is one key per request.
    let mut v = base.clone();
    v["external_lookups"]["limits"] = http;
    v["external_lookups"]["limits"]["options"] = json!({"batch_keys": 2});
    assert_eq!(parse(&v).unwrap_err().code, InvalidArgument);
    // drop / cache_negative round-trip on any provider.
    let mut v = base.clone();
    v["external_lookups"]["limits"]["options"] =
        json!({"on_error": "drop", "cache_negative": false});
    let parsed = parse(&v).unwrap();
    let o = &parsed.external_lookups["limits"].options;
    assert!(!o.cache_negative);
    assert_eq!(o.on_error, sparrow_runtime::LookupErrorPolicy::Drop);
}

#[test]
fn redis_validation_checks_target_secret_budget_and_schema() {
    use ErrorCode::*;
    let store = store();
    let secrets = sparrow_connectors::MapSecretResolver::new(
        [("pw".to_string(), "hunter2".to_string())]
            .into_iter()
            .collect(),
    );
    let allowed = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 6379);
    let check = |value: &Value, policy: &sparrow_connectors::TargetPolicy| {
        let spec = parse(value).map_err(|e| e.code)?;
        let plan = crate::bind_plan_with_store(&store, &spec, "p", 1).map_err(|e| e.code)?;
        let schema = crate::stream_to_schema(&store.get_stream(&spec.stream).unwrap())
            .map_err(|e| e.code)?;
        crate::validate::validate_io_with_plan(&spec, &schema, &plan, &secrets, policy, None)
            .map_err(|e| e.code)
    };
    let (_dir, path) = input(1);
    let base = spec(&path, 6379);
    check(&base, &allowed).unwrap();
    assert_eq!(
        check(
            &base,
            &sparrow_connectors::TargetPolicy::allow("127.0.0.1", 1)
        ),
        Err(PolicyDenied),
        "sink and lookup targets are policy-checked"
    );
    let cases: Vec<(&str, Mutation, ErrorCode)> = vec![
        (
            "password over plain redis",
            Box::new(|v| v["sink"]["redis"]["password_secret"] = json!("pw")),
            PolicyDenied,
        ),
        (
            "missing secret",
            Box::new(|v| {
                v["sink"]["redis"]["url"] = json!("rediss://127.0.0.1:6379");
                v["sink"]["redis"]["password_secret"] = json!("nope");
            }),
            SecretMissing,
        ),
        (
            "lookup missing secret",
            Box::new(|v| {
                v["external_lookups"]["limits"]["redis"]["url"] = json!("rediss://127.0.0.1:6379");
                v["external_lookups"]["limits"]["redis"]["password_secret"] = json!("nope");
            }),
            SecretMissing,
        ),
        (
            "userinfo in url",
            Box::new(|v| v["sink"]["redis"]["url"] = json!("redis://u:p@127.0.0.1:6379")),
            InvalidArgument,
        ),
        (
            "db out of range",
            Box::new(|v| v["sink"]["redis"]["url"] = json!("redis://127.0.0.1:6379/4096")),
            InvalidArgument,
        ),
        (
            "bad ca",
            Box::new(|v| {
                v["sink"]["redis"]["url"] = json!("rediss://127.0.0.1:6379");
                v["sink"]["redis"]["ca_pem"] = json!("not a pem");
            }),
            InvalidArgument,
        ),
        (
            "pipeline_bytes",
            Box::new(|v| v["sink"]["redis"]["pipeline_bytes"] = json!(512)),
            BoundExceeded,
        ),
        (
            "pipeline_bytes max",
            Box::new(|v| v["sink"]["redis"]["pipeline_bytes"] = json!(u64::MAX)),
            BoundExceeded,
        ),
        (
            "pipeline_rows max",
            Box::new(|v| v["sink"]["redis"]["pipeline_rows"] = json!(u64::MAX)),
            BoundExceeded,
        ),
        (
            "budget",
            Box::new(|v| {
                v["sink"]["redis"]["pipeline_bytes"] = json!(4 << 20);
                v["sink"]["redis"]["fields"] = Value::Null;
                v["sink"]["redis"].as_object_mut().unwrap().remove("fields");
                v["sink"]["redis"]["field"] = json!("f");
            }),
            BoundExceeded,
        ),
        (
            "retries",
            Box::new(|v| v["sink"]["redis"]["max_retries"] = json!(21)),
            BoundExceeded,
        ),
        (
            "unknown column",
            Box::new(|v| v["sink"]["redis"]["fields"] = json!(["nope"])),
            InvalidSchema,
        ),
        (
            "unknown key column",
            Box::new(|v| v["sink"]["redis"]["key"] = json!("{nope}")),
            InvalidSchema,
        ),
    ];
    for (name, mutate, code) in cases {
        let mut v = base.clone();
        mutate(&mut v);
        assert_eq!(check(&v, &allowed), Err(code), "{name}");
    }
}

async fn run_until(
    sup: &Arc<Supervisor>,
    store: &Store,
    done: impl Fn() -> bool,
) -> crate::store::ActualState {
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                sup.converge_once().await.unwrap();
                let actual = store.actual("redis").unwrap();
                if done() || actual.status == "failed" {
                    break actual;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("pipeline progresses")
    }
}

#[test]
fn redis_lookup_and_sink_end_to_end_through_the_supervisor() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        const N: usize = 10;
        let (_dir, path) = input(N);
        let mock = Mock::start(b":1\r\n").await;
        let store = store();
        store.put_allow("127.0.0.1", mock.port).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        store
            .put_pipeline("redis", &parse(&spec(&path, mock.port)).unwrap(), None)
            .unwrap();
        request_start(&store, "redis", "test").unwrap();
        // The sealed file completes the job; keep the diagnostics handle.
        let diag = std::cell::RefCell::new(None);
        let actual = run_until(&sup, &store, || {
            if diag.borrow().is_none() {
                *diag.borrow_mut() = sup.flow_snapshot("redis").unwrap().map(|f| f.diagnostics);
            }
            mock.named("HSET").len() >= N / 2
        })
        .await;
        assert_ne!(actual.status, "failed", "{actual:?}");
        let diag = diag.into_inner();
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Hits (even ids) are written; misses have a NULL threshold, so
        // their HSET has no field and is dropped as a bad row.
        let expected: Vec<Vec<String>> = (0..N)
            .step_by(2)
            .map(|i| {
                vec![
                    "HSET".into(),
                    format!("out:d{i}"),
                    "threshold".into(),
                    i.to_string(),
                ]
            })
            .collect();
        assert_eq!(mock.named("HSET"), expected, "order, no duplicates");
        assert_eq!(
            mock.named("SELECT"),
            vec![vec!["SELECT".to_string(), "2".to_string()]]
        );
        let hmget = mock.named("HMGET");
        assert_eq!(hmget.len(), N, "every key looked up once (cache_ttl_ms 0)");
        assert!(hmget.iter().all(|a| a[2] == "threshold"));
        let io = diag.expect("flow observed").snapshot();
        assert_eq!(io.redis_sink_commands_ok, (N / 2) as u64, "{io:?}");
        assert_eq!(io.redis_sink_dropped_bad, (N / 2) as u64, "{io:?}");
        assert_eq!(io.redis_sink_fatal, 0);
        sup.stop_all().await;
    });
}

#[test]
fn redis_auth_rejection_fails_the_job() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let (_dir, path) = input(4);
        let mock = Mock::start(b"-NOAUTH Authentication required.\r\n").await;
        let store = store();
        store.put_allow("127.0.0.1", mock.port).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        store
            .put_pipeline("redis", &parse(&spec(&path, mock.port)).unwrap(), None)
            .unwrap();
        request_start(&store, "redis", "test").unwrap();
        let actual = run_until(&sup, &store, || false).await;
        assert_eq!(actual.status, "failed");
        let error = actual.last_error.unwrap_or_default();
        assert!(error.contains("Redis Sink stopped the job"), "{error}");
        // At most one pipeline (the two hits) was written before the first
        // reply; no command is re-sent after NOAUTH.
        let hset = mock.named("HSET");
        assert!((1..=2).contains(&hset.len()), "{hset:?}");
        assert!(
            hset.len() == 1 || hset[0][1] != hset[1][1],
            "no resend: {hset:?}"
        );
        sup.stop_all().await;
    });
}

//! Full-service, sink-verified benchmark. No AlignedSession shortcut or Python.
//! Both engines use one Mosquitto broker, reusable publishers and this capture.
#![recursion_limit = "256"]
use reqwest::{Client, Method};
use serde_json::{json, Value};
use sparrow_connectors::{CapturedRequest, HttpCapture, MqttPublisher};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const TOKEN: &str = "isolated-benchmark-only";
const WINDOW: usize = 8;
const CHUNK: usize = 400;
const SCENARIOS: &[&str] = &[
    "mqtt_filter_http",
    "file_count_window",
    "mqtt_http_queue_pressure",
    "file_chunked_no_checkpoint",
    "file_chunked_checkpoint",
];

struct Options {
    engine: String,
    ekuiper: Option<PathBuf>,
    server: PathBuf,
    mosquitto: PathBuf,
    out: PathBuf,
    rounds: usize,
    mqtt_events: usize,
    file_events: usize,
    rate: usize,
    checkpoint_events: usize,
    pressure_events: usize,
    drain_secs: u64,
    scenarios: Vec<String>,
    window: usize,
    sink_delay_ms: u64,
    metrics_ms: u64,
    broker_nodelay: bool,
    inbox_wait_ms: Option<u64>,
    source_tuning: serde_json::Map<String, Value>,
    sink_tuning: serde_json::Map<String, Value>,
    observe_ms: u64,
    checkpoint_before_sink: bool,
    broker_pid: u32,
}
impl Options {
    fn parse() -> Result<Self> {
        let mut o = Self {
            engine: "sparrow".into(),
            ekuiper: None,
            server: std::env::current_exe()?
                .parent()
                .unwrap()
                .join("sparrow-server"),
            mosquitto: "/usr/sbin/mosquitto".into(),
            out: PathBuf::from(format!(
                "/tmp/sparrow-bench-v2-{}-{}",
                std::process::id(),
                unix_us()
            )),
            rounds: 3,
            mqtt_events: 10_000,
            file_events: 80_000,
            rate: 1000,
            checkpoint_events: 8000,
            pressure_events: 1024,
            drain_secs: 15,
            scenarios: SCENARIOS.iter().map(|s| (*s).into()).collect(),
            window: WINDOW,
            sink_delay_ms: 0,
            metrics_ms: 0,
            broker_nodelay: false,
            inbox_wait_ms: None,
            source_tuning: serde_json::Map::new(),
            sink_tuning: serde_json::Map::new(),
            observe_ms: 0,
            checkpoint_before_sink: false,
            broker_pid: 0,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--help" {
                println!("Optional diagnostic sampling: --metrics-ms 100 (0 disables; snapshots add measurement overhead)");
                println!("Broker transport control: --broker-nodelay true|false (default false, recorded in metadata)");
                println!("Sparrow ingress control: --inbox-wait-ms 0..1000 (omitted: server default; 0: immediate drop)");
                println!("Tuning: --quickack true|false --inbox-capacity N --inbox-bytes N --http-batch-rows N --http-batch-bytes N --http-linger-ms N --http-max-inflight N");
                println!("External buffers/CPU: --observe-ms 200 (Linux ss samples + broker $SYS at 1s; 0 disables)");
                println!("sparrow_bench --engine sparrow|ekuiper|both [--ekuiper-dir DIR] [--server-bin PATH]\n  [--mosquitto PATH] [--out NEW_DIR] [--rounds 3] [--mqtt-events 10000]\n  [--file-events 80000] [--rate 1000] [--checkpoint-events 8000]\n  [--pressure-events 1024] [--drain-secs 15] [--quick]\n  [--scenarios comma,separated,names] [--window 8] [--sink-delay-ms 0]\nOwns isolated engine/broker processes; never stops existing services.");
                std::process::exit(0);
            }
            if arg == "--quick" {
                o.rounds = 1;
                o.mqtt_events = 512;
                o.file_events = 8000;
                o.checkpoint_events = 800;
                o.pressure_events = 128;
                o.drain_secs = 3;
                continue;
            }
            let value = args.next().ok_or("option requires a value")?;
            match arg.as_str() {
                "--engine" => o.engine = value,
                "--ekuiper-dir" => o.ekuiper = Some(value.into()),
                "--server-bin" => o.server = value.into(),
                "--mosquitto" => o.mosquitto = value.into(),
                "--out" => o.out = value.into(),
                "--rounds" => o.rounds = value.parse()?,
                "--mqtt-events" => o.mqtt_events = value.parse()?,
                "--file-events" => o.file_events = value.parse()?,
                "--checkpoint-events" => o.checkpoint_events = value.parse()?,
                "--pressure-events" => o.pressure_events = value.parse()?,
                "--rate" => o.rate = value.parse()?,
                "--drain-secs" => o.drain_secs = value.parse()?,
                "--scenarios" => o.scenarios = value.split(',').map(str::to_owned).collect(),
                "--window" => o.window = value.parse()?,
                "--sink-delay-ms" => o.sink_delay_ms = value.parse()?,
                "--metrics-ms" => o.metrics_ms = value.parse()?,
                "--broker-nodelay" => o.broker_nodelay = value.parse()?,
                "--inbox-wait-ms" => o.inbox_wait_ms = Some(value.parse()?),
                "--quickack" => {
                    o.source_tuning
                        .insert("tcp_quickack".into(), json!(value.parse::<bool>()?));
                }
                "--inbox-capacity" => {
                    o.source_tuning
                        .insert("inbox_capacity".into(), json!(value.parse::<usize>()?));
                }
                "--inbox-bytes" => {
                    o.source_tuning
                        .insert("inbox_bytes".into(), json!(value.parse::<usize>()?));
                }
                "--http-batch-rows" => {
                    o.sink_tuning
                        .insert("batch_rows".into(), json!(value.parse::<usize>()?));
                }
                "--http-batch-bytes" => {
                    o.sink_tuning
                        .insert("batch_bytes".into(), json!(value.parse::<usize>()?));
                }
                "--http-linger-ms" => {
                    o.sink_tuning
                        .insert("linger_ms".into(), json!(value.parse::<u64>()?));
                }
                "--http-max-inflight" => {
                    o.sink_tuning
                        .insert("max_inflight".into(), json!(value.parse::<usize>()?));
                }
                "--observe-ms" => o.observe_ms = value.parse()?,
                "--checkpoint-before-sink" => o.checkpoint_before_sink = value.parse()?,
                _ => return Err(format!("unknown option {arg}").into()),
            }
        }
        if !["sparrow", "ekuiper", "both"].contains(&o.engine.as_str())
            || o.rounds == 0
            || o.rounds > 20
            || o.rate == 0
            || o.rate > 100_000
            || o.drain_secs == 0
            || o.drain_secs > 300
        {
            return Err("invalid engine, rounds, rate or drain duration".into());
        }
        for n in [
            o.mqtt_events,
            o.file_events,
            o.checkpoint_events,
            o.pressure_events,
        ] {
            if n < 8 || n > 2_000_000 || n % 8 != 0 {
                return Err("event counts must be multiples of 8 in 8..=2000000".into());
            }
        }
        if o.engine != "sparrow" && o.ekuiper.is_none() {
            return Err("--ekuiper-dir is required".into());
        }
        if o.inbox_wait_ms.is_some_and(|ms| ms > 1000) {
            return Err("--inbox-wait-ms must be in 0..=1000".into());
        }
        if o.observe_ms != 0 && !(100..=60000).contains(&o.observe_ms) {
            return Err("--observe-ms must be 0 or 100..60000".into());
        }
        if o.scenarios.is_empty()
            || o.window < 8
            || o.window > 10_000
            || o.window % 8 != 0
            || o.sink_delay_ms > 1000
            || (o.metrics_ms != 0 && !(100..=60_000).contains(&o.metrics_ms))
            || o.scenarios
                .iter()
                .enumerate()
                .any(|(i, s)| !SCENARIOS.contains(&s.as_str()) || o.scenarios[..i].contains(s))
        {
            return Err(
                "invalid scenarios, window (8..=10000, multiple of 8) or sink delay (0..=1000)"
                    .into(),
            );
        }
        for scenario in &o.scenarios {
            if scenario.starts_with("file") {
                let n = if scenario.starts_with("file_chunked") {
                    o.checkpoint_events
                } else {
                    o.file_events
                };
                if n % o.window != 0 {
                    return Err("selected file event counts must be multiples of --window".into());
                }
            }
        }
        if o.engine == "ekuiper" && o.scenarios.iter().all(|s| s.starts_with("file_chunked")) {
            return Err("selected checkpoint scenarios require Sparrow".into());
        }
        Ok(o)
    }
}
fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
fn port() -> Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}
fn copy_tree(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if entry.file_type()?.is_file() {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}
struct Process(Child);
impl Process {
    fn spawn(mut command: Command, log: &Path) -> Result<Self> {
        let log = File::create(log)?;
        command
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        Ok(Self(command.spawn()?))
    }
    fn id(&self) -> u32 {
        self.0.id()
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Engine {
    name: &'static str,
    api: String,
    process: Process,
    client: Client,
}
impl Engine {
    async fn start(name: &'static str, o: &Options, broker: u16) -> Result<Self> {
        let root = o.out.join(name);
        fs::create_dir_all(&root)?;
        let p = port()?;
        let mut command;
        if name == "sparrow" {
            command = Command::new(&o.server);
            command
                .args([
                    "--bind",
                    &format!("127.0.0.1:{p}"),
                    "--token",
                    TOKEN,
                    "--max-jobs",
                    "1",
                    "--catalog",
                ])
                .arg(root.join("catalog.db"))
                .env("SPARROW_DATA_ROOTS", &o.out)
                .env("RUST_LOG", "warn");
        } else {
            let original = o.ekuiper.as_ref().ok_or("missing ekuiper directory")?;
            for dir in ["bin", "data", "log", "plugins"] {
                fs::create_dir_all(root.join(dir))?;
            }
            fs::copy(original.join("bin/kuiperd"), root.join("bin/kuiperd"))?;
            copy_tree(&original.join("etc"), &root.join("etc"))?;
            copy_tree(&original.join("plugins"), &root.join("plugins"))?;
            fs::write(root.join("etc/mqtt_source.yaml"),format!("default:\n  qos: 0\n  server: tcp://127.0.0.1:{broker}\n  protocolVersion: '3.1.1'\n  useInt64ForWholeNumber: true\n"))?;
            // Read once and stay alive until sink verification, like AppendOnly.
            // Next reread is one hour away; each trial stops before it.
            fs::write(root.join("etc/sources/file.yaml"),format!("default:\n  fileType: lines\n  path: {}\n  interval: 3600000\n  sendInterval: 0\n  parallel: false\n  actionAfterRead: 0\n  hasHeader: false\n",o.out.join("data").display()))?;
            command = Command::new(root.join("bin/kuiperd"));
            command
                .current_dir(&root)
                .env("KUIPER__BASIC__RESTIP", "127.0.0.1")
                .env("KUIPER__BASIC__RESTPORT", p.to_string())
                .env("KUIPER__BASIC__IP", "127.0.0.1")
                .env("KUIPER__BASIC__PORT", port()?.to_string())
                .env("KUIPER__BASIC__PPROF", "false")
                .env("KUIPER__BASIC__PROMETHEUS", "false")
                .env("KUIPER__BASIC__LOGLEVEL", "warn")
                .env("KUIPER__BASIC__FILELOG", "false")
                .env("KUIPER__BASIC__ENABLEPRIVATENET", "true")
                .env("KUIPER__BASIC__ALLOWEXTERNALFILEACCESS", "true")
                .env("KUIPER__SOURCE__HTTPSERVERIP", "127.0.0.1")
                .env("KUIPER__SOURCE__HTTPSERVERPORT", port()?.to_string());
        }
        let process = Process::spawn(command, &root.join("engine.log"))?;
        let engine = Self {
            name,
            api: format!("http://127.0.0.1:{p}"),
            process,
            client: Client::builder().timeout(Duration::from_secs(30)).build()?,
        };
        let ready = if name == "sparrow" {
            "/v1/health"
        } else {
            "/streams"
        };
        for _ in 0..100 {
            if engine.call(Method::GET, ready, None).await.is_ok() {
                return Ok(engine);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "{name} did not start; inspect {}/engine.log",
            root.display()
        )
        .into())
    }
    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self.client.request(method, format!("{}{path}", self.api));
        if self.name == "sparrow" {
            request = request.bearer_auth(TOKEN);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            return Err(format!("{} {path}: {status} {text}", self.name).into());
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }
    async fn configure_stream(
        &self,
        id: &str,
        file: bool,
        broker: u16,
        capture: &HttpCapture,
    ) -> Result<()> {
        if self.name == "sparrow" {
            for port in [capture.port(), broker] {
                self.call(
                    Method::PUT,
                    "/v1/allowlist",
                    Some(json!({"host":"127.0.0.1","port":port})),
                )
                .await?;
            }
            let fields = if file {
                json!([
                    {"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}
                ])
            } else {
                json!([
                    {"name":"device_id","type":"utf8","nullable":false},{"name":"seq","type":"int64","nullable":false},
                    {"name":"temperature","type":"float64","nullable":false},{"name":"ts","type":"int64","nullable":false}
                ])
            };
            self.call(
                Method::PUT,
                &format!("/v1/streams/{id}"),
                Some(json!({"fields":fields})),
            )
            .await?;
        } else {
            let sql = if file {
                format!("CREATE STREAM {id} (device_id STRING, v BIGINT) WITH (DATASOURCE=\"{id}.jsonl\", FORMAT=\"JSON\", TYPE=\"file\")")
            } else {
                format!("CREATE STREAM {id} (device_id STRING, seq BIGINT, temperature FLOAT, ts BIGINT) WITH (DATASOURCE=\"bench/{id}\", FORMAT=\"JSON\", TYPE=\"mqtt\")")
            };
            self.call(Method::POST, "/streams", Some(json!({"sql":sql})))
                .await?;
        }
        Ok(())
    }
    async fn activate(
        &self,
        id: &str,
        file: bool,
        aligned: bool,
        small_queue: bool,
        broker: u16,
        capture: &HttpCapture,
        data: &Path,
        window: usize,
        inbox_wait_ms: Option<u64>,
        source_tuning: &serde_json::Map<String, Value>,
        sink_tuning: &serde_json::Map<String, Value>,
    ) -> Result<()> {
        let sql = if file {
            format!(
                "SELECT device_id, SUM(v) AS s FROM {id} GROUP BY device_id, {}({window})",
                if self.name == "sparrow" {
                    "COUNT_WINDOW"
                } else {
                    "COUNTWINDOW"
                }
            )
        } else {
            format!("SELECT device_id, seq, temperature, ts FROM {id} WHERE temperature > 25")
        };
        if self.name == "sparrow" {
            let mut source = if file {
                json!({"kind":"file","path":data,"file_contract":"append_only"})
            } else {
                json!({"kind":"mqtt","host":"127.0.0.1","port":broker,"topic":format!("bench/{id}"),"client_id":id,"inbox_capacity":if small_queue {8} else {32}})
            };
            if !file {
                source
                    .as_object_mut()
                    .unwrap()
                    .extend(source_tuning.clone());
                if let Some(ms) = inbox_wait_ms {
                    source["inbox_wait_ms"] = json!(ms);
                }
            }
            let mut sink = json!({"kind":"http","url":capture.url(),"outbox_capacity":if small_queue {2} else {32}});
            sink.as_object_mut().unwrap().extend(sink_tuning.clone());
            self.call(Method::PUT,&format!("/v1/pipelines/{id}"),Some(json!({
                "stream":id,"sql":sql,"source":source,"fail_on_decode":true,
                "sink":sink,
                "recovery":if aligned {"aligned"} else {"restart_fresh"},"checkpoint_dir":data.with_extension("checkpoints")
            }))).await?;
            self.call(
                Method::POST,
                &format!("/v1/pipelines/{id}/start"),
                Some(json!({})),
            )
            .await?;
        } else {
            self.call(Method::POST,"/rules",Some(json!({"id":id,"sql":sql,
                "options":{"bufferLength":if small_queue {8} else {32},"concurrency":1,"qos":0,"sendError":true,"disableBufferFullDiscard":file},
                "actions":[{"rest":{"url":capture.url(),"method":"POST","sendSingle":false,"bodyType":"json","timeout":1500}}]
            }))).await?;
        }
        Ok(())
    }
    async fn ready(&self, id: &str) -> Result<()> {
        if self.name == "sparrow" {
            let mut running = false;
            for _ in 0..100 {
                let status = self
                    .call(Method::GET, &format!("/v1/pipelines/{id}/status"), None)
                    .await?;
                if status["actual"]["status"] == "running" {
                    running = true;
                    break;
                }
                if status["actual"]["status"] == "failed" {
                    return Err(status.to_string().into());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if !running {
                return Err(format!("pipeline {id} did not start").into());
            }
        }
        // Subscription setup is outside MQTT timing; warmups must also validate.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }
    async fn metrics(&self, id: &str) -> Result<Value> {
        let path = if self.name == "sparrow" {
            "/v1/metrics".to_string()
        } else {
            format!("/rules/{id}/status")
        };
        self.call(Method::GET, &path, None).await
    }
    async fn stop(&self, id: &str) -> Result<()> {
        if self.name == "sparrow" {
            self.call(
                Method::POST,
                &format!("/v1/pipelines/{id}/stop"),
                Some(json!({})),
            )
            .await?;
            for _ in 0..100 {
                let status = self
                    .call(Method::GET, &format!("/v1/pipelines/{id}/status"), None)
                    .await?;
                if status["actual"]["status"] == "stopped" {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            return Err("Sparrow did not stop".into());
        }
        self.call(Method::DELETE, &format!("/rules/{id}"), None)
            .await?;
        self.call(Method::DELETE, &format!("/streams/{id}"), None)
            .await?;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Mqtt,
    File,
}
struct Check {
    kind: Kind,
    input: usize,
    window: usize,
    seen: Vec<bool>,
    sent: Vec<Option<(Instant, u64)>>,
    unique: usize,
    duplicate: usize,
    invalid: usize,
    rows: usize,
    requests: usize,
    latencies: Vec<u64>,
    first: Option<Instant>,
    last: Option<Instant>,
}
fn integer(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| {
        v.as_f64()
            .filter(|x| {
                x.is_finite() && *x >= 0.0 && x.fract() == 0.0 && *x < 9_007_199_254_740_992.0
            })
            .map(|x| x as u64)
    })
}
impl Check {
    fn new(kind: Kind, input: usize, window: usize) -> Self {
        Self {
            kind,
            input,
            window,
            seen: vec![
                false;
                if kind == Kind::File {
                    input / window
                } else {
                    input
                }
            ],
            sent: vec![None; if kind == Kind::Mqtt { input } else { 0 }],
            unique: 0,
            duplicate: 0,
            invalid: 0,
            rows: 0,
            requests: 0,
            latencies: Vec::new(),
            first: None,
            last: None,
        }
    }
    fn expected(&self) -> usize {
        if self.kind == Kind::File {
            self.input / self.window
        } else {
            self.input - (self.input + 3) / 4
        }
    }
    fn drain(&mut self, capture: &HttpCapture) {
        for request in capture.drain_requests() {
            self.accept(request);
        }
    }
    fn accept(&mut self, request: CapturedRequest) {
        self.requests += 1;
        let rows = match serde_json::from_slice::<Value>(&request.body) {
            Ok(Value::Array(rows)) => rows,
            Ok(row @ Value::Object(_)) => vec![row],
            _ => {
                self.invalid += 1;
                return;
            }
        };
        for row in rows {
            self.rows += 1;
            let index = if self.kind == Kind::File {
                let stride = (self.window * self.window) as u64;
                let base = (self.window * (self.window - 1) / 2) as u64;
                integer(&row["s"])
                    .filter(|s| *s >= base && (*s - base) % stride == 0)
                    .map(|s| ((s - base) / stride) as usize)
                    .filter(|w| *w < self.seen.len() && row["device_id"] == format!("d{}", w % 8))
            } else {
                integer(&row["seq"])
                    .map(|s| s as usize)
                    .filter(|i| *i < self.input && i % 4 != 0)
                    .filter(|i| {
                        row["device_id"] == "edge"
                            && row["temperature"].as_f64() == Some(30.0)
                            && self.sent[*i].is_some_and(|(_, ts)| integer(&row["ts"]) == Some(ts))
                    })
            };
            let Some(index) = index else {
                self.invalid += 1;
                continue;
            };
            if self.seen[index] {
                self.duplicate += 1;
                continue;
            }
            self.seen[index] = true;
            self.unique += 1;
            self.first = Some(
                self.first
                    .map_or(request.received_at, |t| t.min(request.received_at)),
            );
            self.last = Some(
                self.last
                    .map_or(request.received_at, |t| t.max(request.received_at)),
            );
            if self.kind == Kind::Mqtt {
                match request
                    .received_at
                    .checked_duration_since(self.sent[index].unwrap().0)
                {
                    Some(d) => self.latencies.push(d.as_micros() as u64),
                    None => self.invalid += 1,
                }
            }
        }
    }
    fn valid(&self) -> bool {
        self.unique == self.expected() && self.duplicate == 0 && self.invalid == 0
    }
    fn hash(&self) -> String {
        let mut h = 0xcbf29ce484222325u64;
        for (i, seen) in self.seen.iter().enumerate() {
            if !seen {
                continue;
            }
            let normalized = if self.kind == Kind::File {
                let w = self.window as u64;
                format!("d{}:{}\n", i % 8, w * w * i as u64 + w * (w - 1) / 2)
            } else {
                format!("edge:{i}:30\n")
            };
            for b in normalized.bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
        }
        format!("{h:016x}")
    }
}
fn percentile(sorted: &[u64], p: f64) -> Option<u64> {
    if sorted.is_empty() {
        None
    } else {
        Some(sorted[((sorted.len() - 1) as f64 * p).round() as usize])
    }
}

fn proc_memory(pid: u32) -> Option<(u64, u64)> {
    let s = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let field = |key: &str| {
        s.lines().find_map(|line| {
            line.strip_prefix(key)?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
    };
    Some((field("VmRSS:")?, field("VmHWM:")?))
}
struct Sampling {
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<Value>>,
}

struct MetricsSampling {
    stop: CancellationToken,
    join: Option<tokio::task::JoinHandle<Result<()>>>,
}

struct SocketSampling {
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<Result<()>>>,
}
impl SocketSampling {
    fn start(
        pid: u32,
        broker_pid: u32,
        broker: u16,
        http: u16,
        path: &Path,
        period_ms: u64,
    ) -> Result<Option<Self>> {
        if period_ms == 0 {
            return Ok(None);
        }
        let mut log = BufWriter::new(File::create(path)?);
        let stop = Arc::new(AtomicBool::new(false));
        let child_stop = stop.clone();
        let filter = format!(
            "( sport = :{broker} or dport = :{broker} or sport = :{http} or dport = :{http} )"
        );
        let join = std::thread::spawn(move || -> Result<()> {
            use std::io::Read;
            let start = Instant::now();
            let cpu = |pid: u32| -> Option<(u64, u64)> {
                let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
                let rest: Vec<_> = stat.rsplit_once(") ")?.1.split_whitespace().collect();
                Some((rest.get(11)?.parse().ok()?, rest.get(12)?.parse().ok()?))
            };
            while !child_stop.load(Ordering::Relaxed) {
                let observed = match Command::new("ss")
                    .args(["-tinmp", &filter])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    Ok(mut process) => {
                        let since = Instant::now();
                        let mut timed_out = false;
                        loop {
                            if process.try_wait()?.is_some() {
                                break;
                            }
                            if child_stop.load(Ordering::Relaxed)
                                || since.elapsed() >= Duration::from_secs(2)
                            {
                                let _ = process.kill();
                                timed_out = true;
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        let status = process.wait()?;
                        let mut bytes = Vec::new();
                        process
                            .stdout
                            .take()
                            .unwrap()
                            .take(128 * 1024)
                            .read_to_end(&mut bytes)?;
                        json!({"ss":String::from_utf8_lossy(&bytes),"exit_success":status.success(),"timed_out":timed_out,"truncated":bytes.len()==128*1024})
                    }
                    Err(error) => json!({"sample_error":error.to_string()}),
                };
                writeln!(
                    log,
                    "{}",
                    json!({"elapsed_us":start.elapsed().as_micros() as u64,"engine_cpu_ticks":cpu(pid),"broker_cpu_ticks":cpu(broker_pid),"broker_rss_kib":proc_memory(broker_pid).map(|m|m.0),"snapshot":observed})
                )?;
                std::thread::park_timeout(Duration::from_millis(period_ms));
            }
            log.flush()?;
            Ok(())
        });
        Ok(Some(Self {
            stop,
            join: Some(join),
        }))
    }
    fn finish(mut self) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        let join = self.join.take().unwrap();
        join.thread().unpark();
        join.join()
            .map_err(|_| std::io::Error::other("socket observer panicked"))??;
        Ok(())
    }
}
impl Drop for SocketSampling {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            join.thread().unpark();
            let _ = join.join();
        }
    }
}

struct BrokerMetrics {
    cancel: CancellationToken,
    join: Option<tokio::task::JoinHandle<Result<()>>>,
}
impl BrokerMetrics {
    async fn start(port: u16, path: &Path) -> Result<Self> {
        use sparrow_connectors::mqtt::{
            codec::{Connect, Packet},
            io::{connect_plain, write_packet, MqttFramedReader},
        };
        let mut log = BufWriter::new(File::create(path)?);
        let mut stream = connect_plain(
            "127.0.0.1",
            port,
            &sparrow_connectors::TlsConfig::disabled(),
            Duration::from_secs(2),
        )
        .await?;
        let mut reader = MqttFramedReader::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            write_packet(
                &mut stream,
                &Packet::Connect(Connect {
                    client_id: "bench-sys-observer".into(),
                    clean_session: true,
                    keepalive: 30,
                    username: None,
                    password: None,
                }),
            )
            .await?;
            if !matches!(
                reader.next(&mut stream).await?,
                Packet::ConnAck { return_code: 0, .. }
            ) {
                return Err("SYS observer CONNACK".into());
            }
            write_packet(
                &mut stream,
                &Packet::Subscribe {
                    packet_id: 1,
                    topics: vec![("$SYS/broker/#".into(), 0)],
                },
            )
            .await?;
            if !matches!(reader.next(&mut stream).await?, Packet::SubAck { .. }) {
                return Err("SYS observer SUBACK".into());
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let join = tokio::spawn(async move {
            let start = Instant::now();
            let mut ping = tokio::time::interval(Duration::from_secs(15));
            loop {
                tokio::select! {
                    biased;
                    _ = child.cancelled() => break,
                    _ = ping.tick() => write_packet(&mut stream, &Packet::PingReq).await?,
                    packet = reader.next(&mut stream) => match packet? {
                        Packet::Publish(p) => {
                            writeln!(log, "{}", json!({"elapsed_us":start.elapsed().as_micros() as u64,"unix_us":unix_us(),"topic":p.topic,"value":String::from_utf8_lossy(&p.payload)}))?;
                        }
                        Packet::Disconnect => return Err("SYS observer disconnected".into()),
                        _ => {},
                    }
                }
            }
            let _ = tokio::time::timeout(
                Duration::from_millis(400),
                write_packet(&mut stream, &Packet::Disconnect),
            )
            .await;
            log.flush()?;
            Ok(())
        });
        Ok(Self {
            cancel,
            join: Some(join),
        })
    }
    async fn finish(mut self) -> Result<()> {
        self.cancel.cancel();
        self.join.take().unwrap().await??;
        Ok(())
    }
}
impl Drop for BrokerMetrics {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl MetricsSampling {
    fn start(engine: &Engine, id: &str, out: &Path, period_ms: u64) -> Result<Option<Self>> {
        if period_ms == 0 {
            return Ok(None);
        }
        let mut log = BufWriter::new(File::create(
            out.join(format!("{id}-metrics-timeline.jsonl")),
        )?);
        let client = engine.client.clone();
        let url = format!(
            "{}{}",
            engine.api,
            if engine.name == "sparrow" {
                "/v1/metrics".into()
            } else {
                format!("/rules/{id}/status")
            }
        );
        let stop = CancellationToken::new();
        let child = stop.clone();
        let join = tokio::spawn(async move {
            let start = Instant::now();
            let mut ticker = tokio::time::interval(Duration::from_millis(period_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = child.cancelled() => break,
                    _ = ticker.tick() => {}
                }
                let fetch = async {
                    let response = client
                        .get(&url)
                        .bearer_auth(TOKEN)
                        .send()
                        .await?
                        .error_for_status()?;
                    response.json::<Value>().await
                };
                let value = tokio::select! {
                    biased;
                    _ = child.cancelled() => break,
                    result = fetch => match result {
                        Ok(value) => value,
                        Err(error) => json!({"sample_error":error.to_string()}),
                    }
                };
                writeln!(
                    log,
                    "{}",
                    json!({"elapsed_us":start.elapsed().as_micros() as u64,"metrics":value})
                )?;
            }
            log.flush()?;
            Ok(())
        });
        Ok(Some(Self {
            stop,
            join: Some(join),
        }))
    }
    async fn finish(mut self) -> Result<()> {
        self.stop.cancel();
        self.join.take().unwrap().await??;
        Ok(())
    }
}
impl Drop for MetricsSampling {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
impl Sampling {
    fn start(pid: u32, csv: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let child = stop.clone();
        let join = std::thread::spawn(move || {
            let mut out = BufWriter::new(File::create(csv).expect("RSS sample file"));
            writeln!(out, "elapsed_us,engine_rss_kib,engine_hwm_kib").unwrap();
            let started = Instant::now();
            let baseline = proc_memory(pid);
            let mut peak = baseline.map(|v| v.0);
            let mut hwm = baseline.map(|v| v.1);
            let mut samples = 0;
            loop {
                if let Some((rss, high)) = proc_memory(pid) {
                    peak = Some(peak.unwrap_or(0).max(rss));
                    hwm = Some(high);
                    samples += 1;
                    writeln!(out, "{},{rss},{high}", started.elapsed().as_micros()).unwrap();
                }
                if child.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            out.flush().unwrap();
            json!({"scope":"engine_pid_only","pid":pid,"baseline_rss_kib":baseline.map(|v|v.0),
                "sampled_peak_rss_kib":peak,"process_lifetime_hwm_kib":hwm,"sample_period_ms":5,"samples":samples})
        });
        Self {
            stop,
            join: Some(join),
        }
    }
    fn finish(mut self) -> Value {
        self.stop.store(true, Ordering::Relaxed);
        self.join.take().unwrap().join().unwrap()
    }
}
impl Drop for Sampling {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn write_rows(out: &mut impl Write, start: usize, end: usize, window: usize) -> Result<()> {
    for i in start..end {
        writeln!(out, "{{\"device_id\":\"d{}\",\"v\":{i}}}", (i / window) % 8)?;
    }
    out.flush()?;
    Ok(())
}
async fn collect(check: &mut Check, capture: &HttpCapture, target: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        check.drain(capture);
        if check.unique >= target || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn trial(
    engine: &Engine,
    o: &Options,
    broker: u16,
    scenario: &str,
    round: usize,
    n: usize,
) -> Result<Value> {
    let mqtt = scenario.starts_with("mqtt");
    let pressure = scenario == "mqtt_http_queue_pressure";
    let chunked = scenario.starts_with("file_chunked");
    let checkpoint = scenario == "file_chunked_checkpoint";
    let id = format!("b_{}_{}_{}", engine.name, scenario, round);
    let capture = HttpCapture::start().await?;
    // Identical bodyless ACKs permit pooling in both clients. eKuiper 2.4.1
    // closes an unread nonempty response unless debugResp is enabled.
    capture.set_status(204);
    capture.set_delay_ms(o.sink_delay_ms);
    if pressure {
        capture.set_delay_ms(8);
    }
    let data = o.out.join("data").join(format!("{id}.jsonl"));
    if !mqtt {
        let mut out = BufWriter::new(File::create(&data)?);
        if !chunked {
            write_rows(&mut out, 0, n, o.window)?;
        }
    }
    engine
        .configure_stream(&id, !mqtt, broker, &capture)
        .await?;
    let sampling = Sampling::start(engine.process.id(), o.out.join(format!("{id}-rss.csv")));
    let activation_start = Instant::now();
    engine
        .activate(
            &id,
            !mqtt,
            chunked,
            pressure,
            broker,
            &capture,
            &data,
            o.window,
            o.inbox_wait_ms,
            &o.source_tuning,
            &o.sink_tuning,
        )
        .await?;
    let metric_samples = MetricsSampling::start(engine, &id, &o.out, o.metrics_ms)?;
    let socket_samples = SocketSampling::start(
        engine.process.id(),
        o.broker_pid,
        broker,
        capture.port(),
        &o.out.join(format!("{id}-sockets.jsonl")),
        o.observe_ms,
    )?;
    let mut check = Check::new(if mqtt { Kind::Mqtt } else { Kind::File }, n, o.window);
    let mut start = activation_start;
    let mut publication_end = None;
    let mut checkpoint_ns = Vec::new();
    let timeout = Duration::from_secs(o.drain_secs);
    if mqtt {
        engine.ready(&id).await?;
        let mut publisher =
            MqttPublisher::connect("127.0.0.1", broker, &format!("pub_{id}")).await?;
        start = Instant::now();
        for i in 0..n {
            if !pressure {
                tokio::time::sleep_until(tokio::time::Instant::from_std(
                    start + Duration::from_secs_f64(i as f64 / o.rate as f64),
                ))
                .await;
            }
            let ts = unix_us();
            let body = serde_json::to_vec(
                &json!({"device_id":"edge","seq":i,"temperature":if i%4==0 {20.0} else {30.0},"ts":ts}),
            )?;
            check.sent[i] = Some((Instant::now(), ts));
            publisher.publish(&format!("bench/{id}"), body).await?;
            if i % 128 == 127 {
                check.drain(&capture);
            }
        }
        publication_end = Some(Instant::now());
        publisher.close().await?;
    } else if chunked {
        engine.ready(&id).await?;
        start = Instant::now();
        let mut file = fs::OpenOptions::new().append(true).open(&data)?;
        let baseline_ingest = if checkpoint && o.checkpoint_before_sink {
            engine.metrics(&id).await?["ingested_rows"]
                .as_u64()
                .ok_or("missing ingress counter")?
        } else {
            0
        };
        for from in (0..n).step_by(CHUNK) {
            let end = (from + CHUNK).min(n);
            write_rows(&mut file, from, end, o.window)?;
            if checkpoint && o.checkpoint_before_sink {
                tokio::time::timeout(timeout, async {
                    loop {
                        if engine.metrics(&id).await?["ingested_rows"]
                            .as_u64()
                            .unwrap_or(0)
                            >= baseline_ingest + end as u64
                        {
                            return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await??;
            } else {
                collect(&mut check, &capture, end / o.window, timeout).await;
                if check.unique < end / o.window {
                    break;
                }
            }
            if checkpoint {
                let t = Instant::now();
                engine
                    .call(
                        Method::POST,
                        &format!("/v1/pipelines/{id}/checkpoint"),
                        Some(json!({})),
                    )
                    .await?;
                checkpoint_ns.push(t.elapsed().as_nanos() as u64);
            }
            if checkpoint && o.checkpoint_before_sink {
                collect(&mut check, &capture, end / o.window, timeout).await;
                if check.unique < end / o.window {
                    break;
                }
            }
        }
        publication_end = Some(Instant::now());
    }
    let target = check.expected();
    collect(&mut check, &capture, target, timeout).await;
    // Settle after completion to include late duplicates without charging polling
    // delay to latency. Never infer completion from source EOF or a rule status.
    tokio::time::sleep(Duration::from_millis(200)).await;
    check.drain(&capture);
    let observed_end = Instant::now();
    let metrics = engine.metrics(&id).await?;
    fs::write(
        o.out.join(format!("{id}-metrics.json")),
        serde_json::to_vec_pretty(&metrics)?,
    )?;
    let resource = sampling.finish();
    if let Some(samples) = socket_samples {
        samples.finish()?;
    }
    if let Some(samples) = metric_samples {
        samples.finish().await?;
    }
    let valid = check.valid() && capture.overflows() == 0;
    let end = if valid {
        check.last.into_iter().chain(publication_end).max().unwrap()
    } else {
        observed_end
    };
    let elapsed_ns = end.saturating_duration_since(start).as_nanos().max(1) as u64;
    let hash = check.hash();
    check.latencies.sort_unstable();
    let steady_ns = check
        .first
        .zip(check.last)
        .map(|(a, b)| b.saturating_duration_since(a).as_nanos() as u64)
        .filter(|n| *n > 0);
    let result = json!({"engine":engine.name,"scenario":scenario,"round":round,"warmup":round==0,
        "input_events":n,"window_rows":o.window,"broker_tcp_nodelay":o.broker_nodelay,
        "sparrow_inbox_wait_ms_override":o.inbox_wait_ms,
        "source_tuning":o.source_tuning,"sink_tuning":o.sink_tuning,"observe_ms":o.observe_ms,
        "checkpoint_before_sink":o.checkpoint_before_sink,
        "sink_delay_ms":if pressure {8} else {o.sink_delay_ms},
        "expected_output_rows":check.expected(),"output_rows":check.rows,
        "unique_rows":check.unique,"missing_rows":check.expected().saturating_sub(check.unique),
        "duplicate_rows":check.duplicate,"invalid_rows":check.invalid,"http_requests":check.requests,
        "http_connections":capture.connections(),"capture_overflows":capture.overflows(),
        "mqtt_publisher_connections":if mqtt {1} else {0},"offered_rate":if mqtt && !pressure {Some(o.rate)} else {None},
        "publish_elapsed_ns":publication_end.map(|t|t.saturating_duration_since(start).as_nanos() as u64),
        "elapsed_ns":elapsed_ns,"elapsed_ms":elapsed_ns as f64/1e6,
        "observation_elapsed_ns":observed_end.saturating_duration_since(start).as_nanos() as u64,
        "invalid_timing":"invalid trials include the full drain/settle observation period",
        "timing":if mqtt {"first_publish_to_max_last_publish_or_sink_arrival"} else if chunked {"first_append_to_last_sink_and_checkpoint_ack"} else {"submit_configuration_to_last_sink_arrival"},
        "valid":valid,"stress_case":pressure,"normalized_output_fnv1a64":hash,
        "input_events_per_s":if valid {Some(n as f64*1e9/elapsed_ns as f64)} else {None},
        "delivered_rows_per_s":check.unique as f64*1e9/elapsed_ns as f64,
        "steady_output_rows_per_s":steady_ns.filter(|_|valid).map(|ns|check.unique.saturating_sub(1) as f64*1e9/ns as f64),
        "e2e_p50_us":percentile(&check.latencies,0.50),"e2e_p99_us":percentile(&check.latencies,0.99),
        "latency_samples":check.latencies.len(),"memory":resource,
        "checkpoints":checkpoint_ns.len(),"checkpoint_request_ns":checkpoint_ns});
    engine.stop(&id).await?;
    capture.stop().await;
    Ok(result)
}

async fn run(mut o: Options) -> Result<()> {
    // Never overwrite a previous run or the supplied eKuiper installation.
    fs::create_dir(&o.out)?;
    o.out = o.out.canonicalize()?;
    if o.engine != "ekuiper" {
        o.server = o.server.canonicalize()?;
    }
    fs::create_dir(o.out.join("data"))?;
    let broker_port = port()?;
    let config = o.out.join("mosquitto.conf");
    fs::write(
        &config,
        format!("listener {broker_port} 127.0.0.1\nallow_anonymous true\npersistence false\nset_tcp_nodelay {}\n{}", o.broker_nodelay, if o.observe_ms > 0 {"sys_interval 1\n"} else {""}),
    )?;
    let mut command = Command::new(&o.mosquitto);
    command.arg("-c").arg(&config);
    let broker = Process::spawn(command, &o.out.join("mosquitto.log"))?;
    o.broker_pid = broker.id();
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(publisher) = MqttPublisher::connect("127.0.0.1", broker_port, "readiness").await {
            publisher.close().await?;
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !ready {
        return Err("Mosquitto failed to start".into());
    }
    let broker_metrics = if o.observe_ms > 0 {
        Some(BrokerMetrics::start(broker_port, &o.out.join("broker-sys.jsonl")).await?)
    } else {
        None
    };
    let mut engines = Vec::new();
    if o.engine != "ekuiper" {
        engines.push(Engine::start("sparrow", &o, broker_port).await?);
    }
    if o.engine != "sparrow" {
        engines.push(Engine::start("ekuiper", &o, broker_port).await?);
    }
    let metadata = json!({"format":"sparrow-bench-v2","started_unix_us":unix_us(),"rounds":o.rounds,
        "mqtt_events":o.mqtt_events,"file_events":o.file_events,"offered_rate":o.rate,
        "broker":"Mosquitto; shared for both engines","broker_pid":broker.id(),"broker_port":broker_port,
        "broker_tcp_nodelay":o.broker_nodelay,
        "sparrow_inbox_wait_ms_override":o.inbox_wait_ms,
        "source_tuning":o.source_tuning,"sink_tuning":o.sink_tuning,"observe_ms":o.observe_ms,
        "checkpoint_before_sink":o.checkpoint_before_sink,
        "socket_metrics":"ss sampled snapshots include MQTT and HTTP endpoints; not true peaks or packet traces; correlate endpoints with broker client IDs",
        "broker_metrics":"optional isolated-broker $SYS, includes observer traffic; missing topics are unavailable, never assumed zero",
        "driver_pid":std::process::id(),"memory_scope":"engine PID only; excludes common broker/capture/driver",
        "capture":"shared Rust HTTP/1.1 keep-alive, 204 No Content; complete-body monotonic receive timestamps",
        "queues":"Sparrow inbox/outbox 32; eKuiper bufferLength 32, disableBufferFullDiscard=true for file; pressure 8/2 vs 8 per stage is not an equal total memory budget",
        "file_semantics":"W-row contiguous blocks per key; both yield d(w%8), W*W*w+W*(W-1)/2",
        "window_rows":o.window,"scenarios":o.scenarios,"sink_delay_ms":o.sink_delay_ms,"max_jobs":1,"metrics_period_ms":o.metrics_ms,
        "file_lifecycle":"read once, stay open; next eKuiper reread in 1 hour; stop after sink verification",
        "truth":"no exactly-once, no MQTT replay, invalid trials cannot claim input throughput",
        "engines":engines.iter().map(|e|json!({"name":e.name,"pid":e.process.id(),"api":e.api})).collect::<Vec<_>>()});
    fs::write(
        o.out.join("metadata.json"),
        serde_json::to_vec_pretty(&metadata)?,
    )?;
    let mut raw = File::create(o.out.join("results.jsonl"))?;
    let mut results = Vec::new();
    for scenario in o.scenarios.iter().map(String::as_str) {
        for round in 0..=o.rounds {
            for idx in 0..engines.len() {
                let engine = &engines[if round % 2 == 0 {
                    idx
                } else {
                    engines.len() - 1 - idx
                }];
                if scenario.starts_with("file_chunked") && engine.name != "sparrow" {
                    continue;
                }
                let base = match scenario {
                    "mqtt_filter_http" => o.mqtt_events,
                    "file_count_window" => o.file_events,
                    "mqtt_http_queue_pressure" => o.pressure_events,
                    _ => o.checkpoint_events,
                };
                let n = if round == 0 {
                    let unit = if scenario.starts_with("file") {
                        o.window
                    } else {
                        8
                    };
                    (base / 10 / unit)
                        .max(128usize.div_ceil(unit))
                        .min(base / unit)
                        * unit
                } else {
                    base
                };
                let result = trial(engine, &o, broker_port, scenario, round, n).await?;
                writeln!(raw, "{}", serde_json::to_string(&result)?)?;
                raw.flush()?;
                eprintln!(
                    "{} {scenario} round={round} valid={} rows={}/{} elapsed_ms={:.2}",
                    engine.name,
                    result["valid"],
                    result["unique_rows"],
                    result["expected_output_rows"],
                    result["elapsed_ms"].as_f64().unwrap()
                );
                results.push(result);
            }
        }
    }
    if let Some(observer) = broker_metrics {
        observer.finish().await?;
    }
    let mut groups: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for result in &results {
        if result["warmup"] == false {
            groups
                .entry(format!(
                    "{}/{}",
                    result["engine"].as_str().unwrap(),
                    result["scenario"].as_str().unwrap()
                ))
                .or_default()
                .push(result);
        }
    }
    let mut summary = serde_json::Map::new();
    for (key, values) in groups {
        let valid: Vec<_> = values.iter().filter(|v| v["valid"] == true).collect();
        let mut fields = json!({"trials":values.len(),"valid_trials":valid.len()});
        for field in [
            "elapsed_ms",
            "input_events_per_s",
            "e2e_p50_us",
            "e2e_p99_us",
        ] {
            let mut numbers: Vec<_> = valid.iter().filter_map(|v| v[field].as_f64()).collect();
            numbers.sort_by(f64::total_cmp);
            if !numbers.is_empty() {
                fields[field] = json!({"min":numbers[0],"median":numbers[numbers.len()/2],"max":numbers[numbers.len()-1]});
            }
        }
        summary.insert(key, fields);
    }
    fs::write(
        o.out.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("artifacts={}", o.out.display());
    if results
        .iter()
        .any(|v| v["stress_case"] == false && v["valid"] == false)
    {
        return Err("incomplete/invalid delivery: inspect results.jsonl; invalid trials have no throughput claim".into());
    }
    Ok(())
}
fn main() {
    let result = Options::parse().and_then(|o| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(Into::into)
            .and_then(|rt| rt.block_on(run(o)))
    });
    if let Err(error) = result {
        eprintln!("sparrow_bench: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sink_light_windows_validate_exact_sums_and_keys() {
        for window in [8, 80, 800] {
            let input = window * 16;
            let mut bytes = Vec::new();
            write_rows(&mut bytes, 0, input, window).unwrap();
            let text = String::from_utf8(bytes).unwrap();
            let rows: Vec<Value> = text
                .lines()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect();
            let mut check = Check::new(Kind::File, input, window);
            for chunk in rows.chunks(window) {
                assert!(chunk
                    .iter()
                    .all(|r| r["device_id"] == chunk[0]["device_id"]));
                let sum: u64 = chunk.iter().map(|r| r["v"].as_u64().unwrap()).sum();
                check.accept(CapturedRequest {
                    body: serde_json::to_vec(&json!({"device_id":chunk[0]["device_id"],"s":sum}))
                        .unwrap(),
                    received_at: Instant::now(),
                });
            }
            assert!(check.valid());
            assert_eq!(check.unique, 16);
        }
    }
    #[test]
    fn receive_time_not_poll_time_and_batch_rows_are_counted() {
        let mut check = Check::new(Kind::Mqtt, 8, WINDOW);
        let sent = Instant::now();
        for i in 0..8 {
            check.sent[i] = Some((sent, 100 + i as u64));
        }
        let rows: Vec<_> = (0..8)
            .filter(|i| i % 4 != 0)
            .map(|i| json!({"device_id":"edge","seq":i,"temperature":30,"ts":100+i}))
            .collect();
        check.accept(CapturedRequest {
            body: serde_json::to_vec(&rows).unwrap(),
            received_at: sent + Duration::from_millis(3),
        });
        assert!(check.valid());
        assert_eq!(check.requests, 1);
        assert_eq!(check.unique, 6);
        assert_eq!(percentile(&check.latencies, 0.99), Some(3000));
    }
    #[test]
    fn exact_windows_reject_missing_duplicate_and_wrong_values() {
        let mut check = Check::new(Kind::File, 16, WINDOW);
        let at = Instant::now();
        let ack = |body: &str| CapturedRequest {
            body: body.as_bytes().to_vec(),
            received_at: at,
        };
        check.accept(ack(r#"{"device_id":"d1","s":92}"#));
        assert!(!check.valid());
        check.accept(ack(r#"[{"device_id":"d0","s":28}]"#));
        assert!(check.valid());
        check.accept(ack(r#"{"device_id":"d0","s":28}"#));
        assert!(!check.valid());
        assert_eq!(check.duplicate, 1);
        check.accept(ack(r#"{"device_id":"d0","s":29}"#));
        assert_eq!(check.invalid, 1);
    }
}

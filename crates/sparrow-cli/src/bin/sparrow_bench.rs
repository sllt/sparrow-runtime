//! End-to-end product benchmark (host-specific, not an SLO).
//!
//! Scenarios:
//! 1. MQTT → Filter/Project → HTTP sink (embedded broker + capture)
//! 2. File/replay → count window → aligned session (throughput)
//! 3. Same file path with a checkpoint every K events (overhead)
//!
//! Does **not** claim exactly-once. MQTT replay remains unsupported.

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sparrow_cli::{rss_kb, LiveLoop};
use sparrow_connectors::{
    publish_qos0_many, sensor_json, FileContract, FileReplayConfig, FileReplaySource,
};
use sparrow_expr::Expr;
use sparrow_io::ReplayableSource;
use sparrow_model::{
    AggFn, DataType, Field, FieldId, OperatorId, RecoveryPolicy, ResourceBudget, Schema, SchemaId,
    WindowKind,
};
use sparrow_plan::{AggCall, WindowSpec};
use sparrow_runtime::{run_until, AlignedSession, CheckpointStore};

const MQTT_EVENTS: usize = 120;
const FILE_EVENTS: usize = 8_000;
const CHECKPOINT_EVERY: u64 = 400;
const MQTT_TIMEOUT: Duration = Duration::from_secs(15);
const SMOKE_MQTT_EPS: f64 = 5.0;
const SMOKE_FILE_EPS: f64 = 500.0;
const SMOKE_P99_US: u64 = 5_000_000;
const SMOKE_RSS_KB: u64 = 512 * 1024;

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn parse_ts(body: &str) -> Option<i64> {
    let key = "\"ts\":";
    let idx = body.find(key)?;
    let rest = &body[idx + key.len()..];
    let digits: String = rest
        .chars()
        .skip_while(|c| !c.is_ascii_digit() && *c != '-')
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().ok()
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn count_schema() -> Schema {
    Schema::new(
        SchemaId::new(1),
        vec![
            Field::new(FieldId::new(1), "device_id", DataType::Utf8, false),
            Field::new(FieldId::new(2), "v", DataType::Int64, false),
        ],
    )
    .unwrap()
}

fn count_spec() -> WindowSpec {
    WindowSpec::new(
        WindowKind::Count { size: 8 },
        vec!["device_id".into()],
        vec![AggCall::new(
            AggFn::Sum,
            Some(Expr::Column { name: "v".into() }),
            "s",
        )],
    )
}

fn tmp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sparrow-bench-{name}-{}-{}",
        std::process::id(),
        now_micros()
    ))
}

struct Scenario {
    name: &'static str,
    events: u64,
    elapsed_ms: u128,
    events_per_s: f64,
    p50_us: Option<u64>,
    p99_us: Option<u64>,
    dropped: u64,
    backpressure: u64,
    peak_rss_kb: Option<u64>,
    extras: String,
    pass: bool,
}

impl Scenario {
    fn print(&self) {
        println!("--- {} ---", self.name);
        println!("events={}", self.events);
        println!("elapsed_ms={}", self.elapsed_ms);
        println!("events_per_s={:.1}", self.events_per_s);
        if let Some(p50) = self.p50_us {
            println!("e2e_p50_us={p50}");
        }
        if let Some(p99) = self.p99_us {
            println!("e2e_p99_us={p99}");
        }
        println!("drops={}", self.dropped);
        println!("backpressure={}", self.backpressure);
        if let Some(rss) = self.peak_rss_kb {
            println!("peak_rss_kb={rss}");
        }
        if !self.extras.is_empty() {
            println!("{}", self.extras);
        }
        println!("smoke={}", if self.pass { "pass" } else { "fail" });
    }
}

fn mqtt_http(kernel: &sparrow_runtime::Kernel) -> sparrow_model::Result<Scenario> {
    let live = LiveLoop::start(kernel, 64, 64)?;
    live.http.set_delay_ms(0);
    let rss0 = rss_kb();
    let started = Instant::now();
    // Stamp send-time into `ts` so p50/p99 are end-to-end, not event-time skew.
    // Pace batches so the embedded broker's bounded fanout is not the story.
    kernel.block_on(async {
        let host = live.broker.host();
        let port = live.broker.port();
        for i in 0..MQTT_EVENTS {
            let payload = sensor_json("edge-flood", 30.0, 40.0, now_micros(), true);
            publish_qos0_many(&host, port, "bench-flood", "sensors/json", vec![payload])
                .await
                .map_err(|e| sparrow_model::SparrowError::new(e.code(), e.to_string()))?;
            if i % 8 == 7 {
                tokio::time::sleep(Duration::from_millis(8)).await;
            }
        }
        Ok::<(), sparrow_model::SparrowError>(())
    })?;
    let mut latencies = Vec::new();
    let mut seen = 0usize;
    let expected = MQTT_EVENTS;
    let mut idle_since = Instant::now();
    kernel.block_on(async {
        let deadline = Instant::now() + MQTT_TIMEOUT;
        while Instant::now() < deadline {
            let bodies = live.http.body_strings();
            if bodies.len() > seen {
                let now = now_micros();
                for body in &bodies[seen..] {
                    if let Some(ts) = parse_ts(body) {
                        latencies.push(now.saturating_sub(ts) as u64);
                    }
                }
                seen = bodies.len();
                idle_since = Instant::now();
            }
            if seen >= expected {
                break;
            }
            if idle_since.elapsed() > Duration::from_millis(600) && seen > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let elapsed = started.elapsed();
    let diag = live.diag.snapshot();
    live.stop(kernel)?;
    latencies.sort_unstable();
    let events = seen as u64;
    let eps = events as f64 / elapsed.as_secs_f64().max(0.001);
    let p50 = if latencies.is_empty() {
        None
    } else {
        Some(percentile(&latencies, 0.50))
    };
    let p99 = if latencies.is_empty() {
        None
    } else {
        Some(percentile(&latencies, 0.99))
    };
    let rss1 = rss_kb();
    let peak = match (rss0, rss1) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        _ => None,
    };
    let drops = diag.mqtt_dropped_full + diag.mqtt_dropped_bad + diag.http_dropped;
    let pressure = diag.http_inflight + diag.http_retries;
    let pass = events >= (MQTT_EVENTS as u64 / 4)
        && eps >= SMOKE_MQTT_EPS
        && p99.unwrap_or(0) <= SMOKE_P99_US
        && peak.unwrap_or(0) <= SMOKE_RSS_KB;
    Ok(Scenario {
        name: "mqtt_filter_http",
        events,
        elapsed_ms: elapsed.as_millis(),
        events_per_s: eps,
        p50_us: p50,
        p99_us: p99,
        dropped: drops,
        backpressure: pressure,
        peak_rss_kb: peak,
        extras: format!(
            "mqtt_recv={} mqtt_decoded={} http_posted={} http_failed={}",
            diag.mqtt_received, diag.mqtt_decoded, diag.http_posted, diag.http_failed
        ),
        pass,
    })
}

fn mqtt_http_pressure(kernel: &sparrow_runtime::Kernel) -> sparrow_model::Result<Scenario> {
    let live = LiveLoop::start(kernel, 8, 2)?;
    live.http.set_delay_ms(8);
    let rss0 = rss_kb();
    let started = Instant::now();
    live.publish_flood(kernel, 80, 30.0)?;
    kernel.block_on(async {
        tokio::time::sleep(Duration::from_millis(400)).await;
    });
    let elapsed = started.elapsed();
    let diag = live.diag.snapshot();
    let posted = live.http.body_strings().len() as u64;
    live.stop(kernel)?;
    let drops = diag.mqtt_dropped_full + diag.http_dropped;
    let rss1 = rss_kb();
    let peak = match (rss0, rss1) {
        (Some(a), Some(b)) => Some(a.max(b)),
        _ => rss0.or(rss1),
    };
    Ok(Scenario {
        name: "mqtt_http_queue_pressure",
        events: posted,
        elapsed_ms: elapsed.as_millis(),
        events_per_s: posted as f64 / elapsed.as_secs_f64().max(0.001),
        p50_us: None,
        p99_us: None,
        dropped: drops,
        backpressure: diag.http_inflight + diag.mqtt_dropped_full + diag.http_dropped,
        peak_rss_kb: peak,
        extras: format!(
            "slow_sink_delay_ms=8 inbox=8 outbox=2 mqtt_full={} http_dropped={}",
            diag.mqtt_dropped_full, diag.http_dropped
        ),
        pass: true,
    })
}

fn write_file(path: &std::path::Path, n: usize) {
    let mut body = String::with_capacity(n * 32);
    for i in 0..n {
        body.push_str(&format!("{{\"device_id\":\"d{}\",\"v\":{}}}\n", i % 8, i));
    }
    std::fs::write(path, body).unwrap();
}

fn file_window(checkpoint_every: Option<u64>) -> sparrow_model::Result<Scenario> {
    let data = tmp("events.ndjson");
    let chk = tmp("chk");
    std::fs::create_dir_all(&chk).unwrap();
    write_file(&data, FILE_EVENTS);
    let cfg = FileReplayConfig {
        path: data.clone(),
        schema: count_schema(),
        restore: sparrow_model::RestoreClaim::None,
        recovery: RecoveryPolicy::Aligned,
        contract: FileContract::Immutable,
    };
    let mut source = FileReplaySource::open(&cfg).map_err(|e| {
        sparrow_model::SparrowError::new(e.code(), e.to_string())
    })?;
    let mut session = AlignedSession::open(
        CheckpointStore::open(&chk)?,
        count_spec(),
        count_schema(),
        OperatorId::new(2),
        ResourceBudget::compact(),
        source.position(),
    )?;
    let rss0 = rss_kb();
    let started = Instant::now();
    let mut ingested = 0u64;
    let mut checkpoints = 0u64;
    loop {
        let batch = if let Some(k) = checkpoint_every {
            run_until(&mut session, &mut source, 0, Some(k))?
        } else {
            run_until(&mut session, &mut source, 0, None)?
        };
        if batch == 0 {
            break;
        }
        ingested += batch;
        if checkpoint_every.is_some() {
            session.checkpoint_barrier()?;
            checkpoints += 1;
        }
        if checkpoint_every.is_none() {
            break;
        }
    }
    let elapsed = started.elapsed();
    let rss1 = rss_kb();
    let peak = match (rss0, rss1) {
        (Some(a), Some(b)) => Some(a.max(b)),
        _ => rss0.or(rss1),
    };
    let eps = ingested as f64 / elapsed.as_secs_f64().max(0.001);
    let name = if checkpoint_every.is_some() {
        "file_count_window_checkpoint"
    } else {
        "file_count_window"
    };
    let _ = std::fs::remove_file(&data);
    let _ = std::fs::remove_dir_all(&chk);
    Ok(Scenario {
        name,
        events: ingested,
        elapsed_ms: elapsed.as_millis(),
        events_per_s: eps,
        p50_us: None,
        p99_us: None,
        dropped: 0,
        backpressure: 0,
        peak_rss_kb: peak,
        extras: format!(
            "finals={} checkpoints={} checkpoint_every={:?} honesty=not_exactly_once",
            session.finals.len(),
            checkpoints,
            checkpoint_every
        ),
        pass: ingested >= FILE_EVENTS as u64 / 2 && eps >= SMOKE_FILE_EPS,
    })
}

fn main() {
    if let Err(e) = run() {
        eprintln!("sparrow_bench failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow end-to-end bench ===");
    println!("host-specific: numbers are not SLOs");
    println!("exactly_once=rejected");
    println!("mqtt_replay=unsupported");
    println!("mqtt_events={MQTT_EVENTS} file_events={FILE_EVENTS} checkpoint_every={CHECKPOINT_EVERY}");
    println!(
        "smoke_thresholds mqtt_eps>={SMOKE_MQTT_EPS} file_eps>={SMOKE_FILE_EPS} p99_us<={SMOKE_P99_US} rss_kb<={SMOKE_RSS_KB}"
    );
    if let Some(rss) = rss_kb() {
        println!("rss_kb_at_start={rss}");
    }

    let kernel = sparrow_cli::compact_kernel()?;
    let mqtt = mqtt_http(&kernel)?;
    mqtt.print();
    let pressure = mqtt_http_pressure(&kernel)?;
    pressure.print();
    let file = file_window(None)?;
    file.print();
    let chk = file_window(Some(CHECKPOINT_EVERY))?;
    chk.print();

    if file.events_per_s > 0.0 && chk.events_per_s > 0.0 {
        let overhead = (file.elapsed_ms as f64 / chk.elapsed_ms.max(1) as f64).recip();
        println!("--- checkpoint_overhead ---");
        println!(
            "no_checkpoint_eps={:.1} checkpoint_eps={:.1} elapsed_ratio={:.2}",
            file.events_per_s,
            chk.events_per_s,
            chk.elapsed_ms as f64 / file.elapsed_ms.max(1) as f64
        );
        let _ = overhead;
    }

    let all = mqtt.pass && pressure.pass && file.pass && chk.pass;
    println!("=== bench smoke: {} ===", if all { "pass" } else { "fail" });
    if !all {
        std::process::exit(1);
    }
    Ok(())
}

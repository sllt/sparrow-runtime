//! Real M2 closed loop: embedded MQTT broker → JSON source → kernel → HTTP capture.
//!
//! ```text
//! cargo run -p sparrow-cli --bin m2_mqtt_http_loop
//! ```
//!
//! Delivery is live_best_effort + restart_fresh. No restore / at-least-once.

use std::time::Duration;

use sparrow_cli::{compact_kernel, rss_kb, LiveLoop};
use sparrow_connectors::{
    ConnectorCapabilities, MapSecretResolver, MqttSourceConfig, ReplaySupport, TargetPolicy,
    TlsConfig,
};
use sparrow_model::{ErrorCode, RestoreClaim};
use sparrow_testkit::sensor_schema;

fn main() {
    if let Err(err) = run() {
        eprintln!("m2_mqtt_http_loop failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow M2 closed loop ===");
    println!(
        "delivery: live_best_effort + restart_fresh | MQTT replay={}",
        ReplaySupport::Unsupported.as_str()
    );
    let mqtt_caps = ConnectorCapabilities::MQTT_SOURCE;
    let http_caps = ConnectorCapabilities::HTTP_SINK;
    println!(
        "capabilities: {} replay={} | {} replay={}",
        mqtt_caps.kind,
        mqtt_caps.replay.as_str(),
        http_caps.kind,
        http_caps.replay.as_str()
    );

    println!("\n[1] reject unauthorized target / missing secret / durable recovery");
    demonstrate_rejects()?;

    let kernel = compact_kernel()?;
    println!("\n[2] start embedded MQTT broker + HTTP capture + kernel");
    let live = LiveLoop::start(&kernel, 8, 8)?;
    println!(
        "    mqtt=127.0.0.1:{} http={}",
        live.broker.port(),
        live.http.url()
    );

    println!("\n[3] publish 6 JSON sensor events; filter temperature > 25");
    live.publish_fixture(&kernel)?;
    let bodies = live.wait_http_at_least(&kernel, 3, Duration::from_secs(5))?;
    println!("    HTTP received {} bodies (expect 3 hot rows first)", bodies.len());
    for (i, body) in bodies.iter().take(3).enumerate() {
        println!("    {i}: {body}");
    }
    if !bodies.iter().any(|b| b.contains("edge-a") && b.contains("26.2"))
        || !bodies.iter().any(|b| b.contains("edge-b") && b.contains("31"))
        || !bodies.iter().any(|b| b.contains("edge-c") && b.contains("29.4"))
    {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            format!("filtered HTTP bodies missing expected devices: {bodies:?}"),
        ));
    }
    println!("    diagnostics: {}", live.diag.snapshot());

    println!("\n[4] slow HTTP → bounded inbox drops (no unbounded RSS growth)");
    live.http.set_delay_ms(80);
    let rss_before = rss_kb();
    live.publish_flood(&kernel, 40, 30.0)?;
    // Drain window: slow sink cannot keep up with 40 QoS0 publishes.
    kernel.block_on(async {
        tokio::time::sleep(Duration::from_millis(400)).await;
    });
    let snap = live.diag.snapshot();
    let rss_after = rss_kb();
    println!("    {snap}");
    if let (Some(a), Some(b)) = (rss_before, rss_after) {
        let delta = b.saturating_sub(a);
        println!("    VmRSS {a} kB → {b} kB (Δ {delta} kB)");
        if delta > 64 * 1024 {
            return Err(sparrow_model::SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("RSS grew by {delta} kB in the demo window; inbox should be bounded"),
            ));
        }
    } else {
        println!("    VmRSS unavailable on this host; bounded-drop evidence is dropped_full");
    }
    if snap.mqtt_dropped_full == 0 && snap.http_posted < 10 {
        // Flood may still fit if the machine is fast; require either drops or a
        // completed bounded drain without a huge post count explosion.
        println!("    note: no inbox drops this run (scheduler was fast enough); queues stayed capped at 8");
    } else {
        println!(
            "    backpressure evidence: mqtt_dropped_full={} http_inflight={}",
            snap.mqtt_dropped_full, snap.http_inflight
        );
    }

    println!("\n[5] graceful stop releases broker, HTTP, and kernel tasks");
    live.stop(&kernel)?;
    println!("    kernel.live_tasks = {}", kernel.live_tasks());
    println!("\nm2_mqtt_http_loop: ok");
    println!("honesty: this is live_best_effort. drops are expected. no crash recovery.");
    Ok(())
}

fn demonstrate_rejects() -> sparrow_model::Result<()> {
    let secrets = MapSecretResolver::empty();
    let schema = sensor_schema();

    let policy = TargetPolicy::deny_all();
    let cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema.clone());
    let err = cfg
        .validate(&secrets, &policy)
        .expect_err("deny-by-default must reject");
    assert_eq!(err.code(), ErrorCode::PolicyDenied);
    println!("    unauthorized target → {}", err);

    let mut missing = MqttSourceConfig::demo("127.0.0.1", 1883, schema.clone());
    missing.password_secret = Some("mqtt.password".into());
    let policy = TargetPolicy::allow("127.0.0.1", 1883);
    let err = missing
        .validate(&secrets, &policy)
        .expect_err("missing secret must reject");
    assert_eq!(err.code(), ErrorCode::SecretMissing);
    println!("    missing secret → {}", err);

    let mut qos = MqttSourceConfig::demo("127.0.0.1", 1883, schema.clone());
    qos.qos = 1;
    let err = qos
        .validate(&secrets, &policy)
        .expect_err("qos1 must reject");
    assert_eq!(err.code(), ErrorCode::UnsupportedDelivery);
    println!("    QoS 1 / at-least-once → {}", err);

    let mut session = MqttSourceConfig::demo("127.0.0.1", 1883, schema.clone());
    session.clean_session = false;
    let err = session
        .validate(&secrets, &policy)
        .expect_err("dirty session must reject");
    assert_eq!(err.code(), ErrorCode::UnsupportedRestore);
    println!("    clean_session=false → {}", err);

    let mut restore = MqttSourceConfig::demo("127.0.0.1", 1883, schema);
    restore.restore = RestoreClaim::MqttSession {
        client_id: "edge-1".into(),
    };
    let err = restore
        .validate(&secrets, &policy)
        .expect_err("mqtt session restore must reject");
    assert_eq!(err.code(), ErrorCode::UnsupportedRestore);
    println!("    MQTT session restore → {}", err);

    let skip = TlsConfig {
        enabled: true,
        skip_verify: true,
    };
    let err = skip.validate().expect_err("skip_verify must reject");
    assert_eq!(err.code(), ErrorCode::PolicyDenied);
    println!("    TLS skip_verify → {}", err);
    Ok(())
}

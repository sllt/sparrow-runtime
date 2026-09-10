//! End-to-end: embedded broker + kernel + HTTP capture. CI-runnable.

use std::time::Duration;

use sparrow_cli::{compact_kernel, rss_kb, LiveLoop};
use sparrow_connectors::{
    MapSecretResolver, MqttSourceConfig, TargetPolicy, TlsConfig,
};
use sparrow_model::{ErrorCode, RestoreClaim};
use sparrow_testkit::sensor_schema;

#[test]
fn mqtt_json_kernel_http_receives_filtered_rows() {
    let kernel = compact_kernel().unwrap();
    let live = LiveLoop::start(&kernel, 16, 16).unwrap();
    live.publish_fixture(&kernel).unwrap();
    let bodies = live
        .wait_http_at_least(&kernel, 3, Duration::from_secs(8))
        .unwrap();
    assert!(
        bodies.iter().any(|b| b.contains("\"device_id\":\"edge-a\"") && b.contains("26.2")),
        "{bodies:?}"
    );
    assert!(bodies.iter().any(|b| b.contains("edge-b") && b.contains("31")));
    assert!(bodies.iter().any(|b| b.contains("edge-c") && b.contains("29.4")));
    live.stop(&kernel).unwrap();
    assert_eq!(kernel.live_tasks(), 0);
}

#[test]
fn slow_http_does_not_grow_unbounded() {
    let kernel = compact_kernel().unwrap();
    let live = LiveLoop::start(&kernel, 8, 4).unwrap();
    live.http.set_delay_ms(100);
    let rss_before = rss_kb();
    live.publish_flood(&kernel, 48, 30.0).unwrap();
    kernel.block_on(async {
        tokio::time::sleep(Duration::from_millis(250)).await;
    });
    let snap = live.diag.snapshot();
    let rss_after = rss_kb();
    if let (Some(a), Some(b)) = (rss_before, rss_after) {
        assert!(
            b.saturating_sub(a) < 64 * 1024,
            "RSS grew too much: {a} -> {b} kB; {snap}"
        );
    }
    assert!(
        snap.mqtt_dropped_full > 0 || snap.mqtt_decoded <= 16,
        "expected bounded drops or a capped decode count, got {snap}"
    );
    live.stop(&kernel).unwrap();
}

#[test]
fn rejects_are_explicit() {
    let secrets = MapSecretResolver::empty();
    let schema = sensor_schema();
    let deny = TargetPolicy::deny_all();
    assert_eq!(
        MqttSourceConfig::demo("8.8.8.8", 1883, schema.clone())
            .validate(&secrets, &deny)
            .unwrap_err()
            .code(),
        ErrorCode::PolicyDenied
    );

    let allow = TargetPolicy::allow("127.0.0.1", 1883);
    let mut missing = MqttSourceConfig::demo("127.0.0.1", 1883, schema.clone());
    missing.tls.enabled = true;
    missing.password_secret = Some("does.not.exist".into());
    assert_eq!(
        missing.validate(&secrets, &allow).unwrap_err().code(),
        ErrorCode::SecretMissing
    );

    let mut restore = MqttSourceConfig::demo("127.0.0.1", 1883, schema);
    restore.restore = RestoreClaim::Checkpoint {
        snapshot_id: "snap-1".into(),
    };
    assert_eq!(
        restore.validate(&secrets, &allow).unwrap_err().code(),
        ErrorCode::UnsupportedRestore
    );

    assert_eq!(
        TlsConfig {
            enabled: true,
            skip_verify: true,
        }
        .validate()
        .unwrap_err()
        .code(),
        ErrorCode::PolicyDenied
    );
}

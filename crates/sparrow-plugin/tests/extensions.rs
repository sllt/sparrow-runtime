#![cfg(all(target_os = "linux", target_env = "gnu"))]
use serde_json::json;
use sparrow_plugin as plugin_runtime;
use sparrow_plugin::{extension::*, *};
use tokio_util::sync::CancellationToken;
#[path = "../../../tests/support/extension_plugin.rs"]
mod fixture;
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_identity_pin_restart_and_explicit_gate() {
    let _serial = SERIAL.lock().unwrap();
    let d = fixture::dir();
    let root = d.0.join("packages");
    let manager = fixture::manager(&root);
    let (manifest, bytes) = fixture::sample("transform");
    let mut binding = fixture::install(&manager, manifest, &bytes);
    binding.config = json!({"factor":2,"copies":2});
    assert!(manager.resolve_extension(&binding, Role::Sink).is_err());
    let extension = manager
        .resolve_extension(&binding, Role::Transform)
        .unwrap();
    let mut session = extension
        .open(&binding.config, CancellationToken::new())
        .unwrap();
    assert_eq!(
        session.transform(vec![Value::Int("21".into())]).unwrap(),
        vec![vec![Value::Int("42".into())]; 2]
    );
    assert_eq!(
        session.transform(vec![Value::Null]).unwrap(),
        vec![vec![Value::Null]; 2]
    );
    assert!(manager.disable(&binding.manifest_sha256).is_err());
    drop(extension);
    assert!(manager.disable(&binding.manifest_sha256).is_err());
    session.close().unwrap();
    drop(session);
    assert_eq!(active_sessions(), 0);
    drop(manager);
    let manager = Manager::open(&root, false).unwrap();
    assert!(!manager.list().unwrap()[0].enabled);
    assert!(manager
        .enable(&binding.manifest_sha256, &binding.manifest_sha256)
        .is_err());
    drop(manager);
    let manager = fixture::manager(&root);
    assert!(manager.list().unwrap()[0].enabled);
    manager.disable(&binding.manifest_sha256).unwrap();
    manager.uninstall(&binding.manifest_sha256).unwrap();
    assert!(manager.list().unwrap().is_empty());
    let (mut wrong, bytes) = fixture::sample("transform");
    wrong
        .package
        .as_mut()
        .unwrap()
        .extension
        .as_mut()
        .unwrap()
        .output[0]
        .name = "wrong".into();
    let hash = manager.install(wrong, &bytes).unwrap().manifest_sha256;
    assert!(manager.enable(&hash, &hash).is_err());
    assert_eq!(active_sessions(), 0);
}
#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_source_admission_sink_flush_and_bounded_rows() {
    let _serial = SERIAL.lock().unwrap();
    let d = fixture::dir();
    let manager = fixture::manager(&d.0.join("packages"));
    let (m, b) = fixture::sample("source");
    let binding = fixture::install(&manager, m, &b);
    let source = manager.resolve_extension(&binding, Role::Source).unwrap();
    let mut source = source
        .open(&json!({"start":21,"count":35}), CancellationToken::new())
        .unwrap();
    let (m, b) = fixture::sample("sink");
    let binding = fixture::install(&manager, m, &b);
    let sink = manager.resolve_extension(&binding, Role::Sink).unwrap();
    let out = d.0.join("rows.ndjson");
    let mut sink = sink
        .open(&json!({"path":out}), CancellationToken::new())
        .unwrap();
    let mut count = 0;
    loop {
        match source.poll().unwrap() {
            Reply::Data { rows, .. } => {
                count += rows.len();
                assert!(source.poll().is_err());
                sink.push(rows).unwrap();
                source.accepted().unwrap();
            }
            Reply::End => break,
            _ => panic!("unexpected poll"),
        }
    }
    assert_eq!(count, 35);
    assert!(source.poll().is_err());
    source.close().unwrap();
    sink.close().unwrap();
    drop(source);
    drop(sink);
    let rows = std::fs::read_to_string(out)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        (21..56).map(|v| json!({"value":v})).collect::<Vec<_>>()
    );
    assert_eq!(active_sessions(), 0);
}
#[test]
#[ignore = "requires separately compiled SDK conformance fixtures"]
fn extensions_faults_timeout_cancel_capacity_and_no_leaked_children() {
    let _serial = SERIAL.lock().unwrap();
    let d = fixture::dir();
    let manager = fixture::manager(&d.0.join("packages"));
    let (m, b) = fixture::fault("transform");
    let binding = fixture::install(&manager, m, &b);
    let extension = manager
        .resolve_extension(&binding, Role::Transform)
        .unwrap();
    for mode in [
        "schema",
        "expansion",
        "sequence",
        "oversize",
        "crash",
        "oom",
        "hang",
    ] {
        let mut session = extension
            .open(&json!({"mode":mode}), CancellationToken::new())
            .unwrap();
        let now = std::time::Instant::now();
        assert!(
            session.transform(vec![Value::Int("1".into())]).is_err(),
            "{mode}"
        );
        drop(session);
        assert!(
            now.elapsed() < std::time::Duration::from_millis(2500),
            "{mode}"
        );
        assert_eq!(active_sessions(), 0);
    }
    let cancel = CancellationToken::new();
    let mut session = extension
        .open(&json!({"mode":"hang"}), cancel.clone())
        .unwrap();
    let stop = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        cancel.cancel();
    });
    let now = std::time::Instant::now();
    assert!(session.transform(vec![Value::Int("1".into())]).is_err());
    drop(session);
    stop.join().unwrap();
    assert!(now.elapsed() < std::time::Duration::from_millis(600));
    let mut sessions = Vec::new();
    for _ in 0..MAX_SESSIONS {
        sessions.push(
            extension
                .open(&json!({"mode":"ok"}), CancellationToken::new())
                .unwrap(),
        );
    }
    assert!(extension
        .open(&json!({"mode":"ok"}), CancellationToken::new())
        .is_err());
    drop(sessions);
    assert_eq!(active_sessions(), 0);
    let mut clean = extension
        .open(&json!({"mode":"environment"}), CancellationToken::new())
        .unwrap();
    assert_eq!(
        clean.transform(vec![Value::Int("1".into())]).unwrap(),
        vec![vec![Value::Int("1".into())]]
    );
    drop(clean);
    let (m, b) = fixture::fault("source");
    let binding = fixture::install(&manager, m, &b);
    let source = manager.resolve_extension(&binding, Role::Source).unwrap();
    let mut source = source
        .open(&json!({"mode":"watermark"}), CancellationToken::new())
        .unwrap();
    assert!(matches!(
        source.poll().unwrap(),
        Reply::Data {
            watermark: Some(10),
            ..
        }
    ));
    source.accepted().unwrap();
    assert!(source.poll().is_err());
    drop(source);
    let (m, b) = fixture::fault("sink");
    let binding = fixture::install(&manager, m, &b);
    let sink = manager.resolve_extension(&binding, Role::Sink).unwrap();
    let mut sink = sink
        .open(&json!({"mode":"flush_fail"}), CancellationToken::new())
        .unwrap();
    assert!(sink.close().is_err());
    drop(sink);
    assert_eq!(active_sessions(), 0);
}

#![cfg(all(target_os = "linux", target_env = "gnu"))]
use sparrow_model::{ErrorCode, Scalar};
use sparrow_plugin::*;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
static SERIAL: Mutex<()> = Mutex::new(());
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn dir() -> Dir {
    let root = std::env::temp_dir().join(format!(
        "sparrow-js-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    Dir(root)
}
fn worker(root: &Dir) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    // Model production `install -m 755`, not Cargo's umask-dependent artifact
    // mode. Never weaken the service's executable permission checks.
    let installed = root.0.join("installed-js-worker");
    if !installed.exists() {
        let sibling = std::env::current_exe()
            .unwrap()
            .with_file_name("sparrow-js-worker");
        let source = std::env::var_os("SPARROW_TEST_JS_WORKER")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                if sibling.is_file() {
                    sibling
                } else {
                    env!("CARGO_BIN_EXE_sparrow-js-worker").into()
                }
            });
        std::fs::copy(source, &installed).unwrap();
        std::fs::set_permissions(&installed, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    installed
}
fn manifest(source: &str, version: &str, ty: ValueType) -> Manifest {
    Manifest {
        format: 1,
        name: "script_test".into(),
        version: version.into(),
        kind: "javascript_scalar".into(),
        abi: 1,
        semantics: 1,
        target: JS_TARGET.into(),
        artifact_sha256: sha256(source.as_bytes()),
        deterministic: true,
        thread_safe: true,
        null_policy: "propagate".into(),
        functions: vec![FunctionDef {
            name: "f".into(),
            id: 1,
            inputs: vec![ty],
            output: ty,
            max_output_bytes: MAX_VALUE,
        }],
    }
}
fn load(root: &Dir, source: &str, ty: ValueType) -> (Arc<Manager>, String, Arc<Function>) {
    let manager =
        Manager::open_with_scripts(&root.0.join("plugins"), false, Some(worker(root))).unwrap();
    let digest = manager
        .install(manifest(source, "v1", ty), source.as_bytes())
        .unwrap()
        .manifest_sha256;
    manager.enable(&digest, &digest).unwrap();
    let function = manager.resolve("script_test", "v1", &digest, "f").unwrap();
    (manager, digest, function)
}
#[test]
fn scripts_all_scalars_exact_bigints_and_null() {
    let _serial = SERIAL.lock().unwrap();
    for (ty, values) in [
        (
            ValueType::Int64,
            vec![Scalar::Int64(i64::MIN), Scalar::Int64(i64::MAX)],
        ),
        (ValueType::UInt64, vec![Scalar::UInt64(u64::MAX)]),
        (
            ValueType::TimestampMicrosUtc,
            vec![Scalar::TimestampMicrosUTC(i64::MIN)],
        ),
        (ValueType::Float64, vec![Scalar::Float64(-0.5)]),
        (ValueType::Bool, vec![Scalar::Bool(true)]),
        (ValueType::Utf8, vec![Scalar::utf8("中\0文😀")]),
        (ValueType::Bytes, vec![Scalar::bytes([0, 1, 128, 255])]),
    ] {
        let d = dir();
        let (_m, _h, f) = load(&d, "({f(v){return v;}})", ty);
        for v in values {
            assert_eq!(f.invoke(std::slice::from_ref(&v)).unwrap(), v);
        }
        assert_eq!(f.invoke(&[Scalar::Null]).unwrap(), Scalar::Null);
    }
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn scripts_registry_pins_restart_upgrade_hot_unload_and_safe_mode() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m, hash, f) = load(&d, "({f(v){return v*2n;}})", ValueType::Int64);
    assert_eq!(f.invoke(&[Scalar::Int64(21)]).unwrap(), Scalar::Int64(42));
    assert!(!m.native_allowed());
    assert!(m.script_allowed());
    assert!(m.disable(&hash).is_err());
    assert!(m.uninstall(&hash).is_err());
    assert!(m.enable(&hash, &"0".repeat(64)).is_err());
    let next = "({f(v){return v*3n;}})";
    assert!(m
        .install(manifest(next, "v1", ValueType::Int64), next.as_bytes())
        .is_err());
    let newer = m
        .install(manifest(next, "v2", ValueType::Int64), next.as_bytes())
        .unwrap()
        .manifest_sha256;
    m.enable(&newer, &newer).unwrap();
    assert_eq!(
        m.resolve("script_test", "v2", &newer, "f")
            .unwrap()
            .invoke(&[Scalar::Int64(21)])
            .unwrap(),
        Scalar::Int64(63)
    );
    assert_eq!(f.invoke(&[Scalar::Int64(21)]).unwrap(), Scalar::Int64(42));
    drop(f);
    drop(m);
    assert_eq!(script::worker_count(), 0);
    let safe = Manager::open(&d.0.join("plugins"), false).unwrap();
    assert!(safe
        .list()
        .unwrap()
        .iter()
        .all(|p| p.desired_enabled && !p.enabled));
    assert!(safe.enable(&hash, &hash).is_err());
    drop(safe);
    let m = Manager::open_with_scripts(&d.0.join("plugins"), false, Some(worker(&d))).unwrap();
    for h in [&hash, &newer] {
        assert!(m.disable(h).unwrap().hot_unload);
        m.uninstall(h).unwrap();
    }
    assert!(m.list().unwrap().is_empty());
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn scripts_reject_bad_outputs_and_forbidden_host_capabilities() {
    let _serial = SERIAL.lock().unwrap();
    for (body, ty) in [
        ("return 9223372036854775808n", ValueType::Int64),
        ("return -1n", ValueType::UInt64),
        ("return 42", ValueType::Int64),
        ("return 0/0", ValueType::Float64),
        ("return '\\ud800'", ValueType::Utf8),
        ("return 'x'.repeat(65537)", ValueType::Utf8),
        ("return new Uint8Array(65537)", ValueType::Bytes),
        ("return Promise.resolve(1n)", ValueType::Int64),
        ("return undefined", ValueType::Int64),
        ("throw new Error('private-details')", ValueType::Int64),
        ("return eval('1n')", ValueType::Int64),
        (
            "return (()=>{}).constructor('return 1n')()",
            ValueType::Int64,
        ),
        (
            "return (function*(){}).constructor('yield 1n')().next().value",
            ValueType::Int64,
        ),
        (
            "return (async function(){}).constructor('return 1n')()",
            ValueType::Int64,
        ),
        (
            "return (async function*(){}).constructor('yield 1n')()",
            ValueType::Int64,
        ),
        (
            "return Reflect.getPrototypeOf([].map).constructor('return 1n')()",
            ValueType::Int64,
        ),
        ("return BigInt(Date.now())", ValueType::Int64),
        ("return BigInt(Math.random())", ValueType::Int64),
        ("return require('fs')", ValueType::Int64),
        ("return fetch('http://127.0.0.1:1')", ValueType::Int64),
    ] {
        let d = dir();
        let (_m, _h, f) = load(&d, &format!("({{f(v){{{body};}}}})"), ty);
        let value = match ty {
            ValueType::UInt64 => Scalar::UInt64(0),
            ValueType::Float64 => Scalar::Float64(0.),
            ValueType::Utf8 => Scalar::utf8(""),
            ValueType::Bytes => Scalar::bytes([]),
            _ => Scalar::Int64(0),
        };
        let e = f.invoke(&[value]).unwrap_err();
        assert!(!e.message.contains("private-details"));
    }
}
#[test]
fn scripts_loop_recursion_deadline_oom_and_worker_recovery() {
    let _serial = SERIAL.lock().unwrap();
    for body in [
        "while(true){}",
        "function recurse(){return recurse()}return recurse()",
        "let n=0;for(let i=0;i<9999;i++){for(let j=0;j<9999;j++){n+=Math.sqrt(j)}}return BigInt(n)",
        "return BigInt(new ArrayBuffer(70*1024*1024).byteLength)",
        "return BigInt(new ArrayBuffer(1024*1024*1024).byteLength)",
    ] {
        let d = dir();
        let (m, h, f) = load(
            &d,
            &format!("({{f(v){{if(v===0n)return 7n;{body};}}}})"),
            ValueType::Int64,
        );
        let begin = Instant::now();
        assert!(f.invoke(&[Scalar::Int64(1)]).is_err(), "{body}");
        assert!(begin.elapsed() < Duration::from_secs(3));
        drop(f);
        m.disable(&h).unwrap();
        assert_eq!(script::worker_count(), 0);
        m.enable(&h, &h).unwrap();
        assert_eq!(
            m.resolve("script_test", "v1", &h, "f")
                .unwrap()
                .invoke(&[Scalar::Int64(0)])
                .unwrap(),
            Scalar::Int64(7)
        );
    }
}
#[test]
fn scripts_cancellation_kills_and_reaps_without_leaking_pin() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m,h,f) = load(&d, "({f(v){let n=0;for(let i=0;i<9999;i++)for(let j=0;j<9999;j++)n+=Math.sqrt(j);return BigInt(n);}})", ValueType::Int64);
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        c.cancel();
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let result = rt.block_on(script::scope(cancel, async {
        f.invoke(&[Scalar::Int64(1)])
    }));
    thread.join().unwrap();
    assert_eq!(result.unwrap_err().code, ErrorCode::Cancelled);
    drop(f);
    m.disable(&h).unwrap();
    m.uninstall(&h).unwrap();
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn scripts_context_isolation_and_bounded_worker_admission() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m,h,f) = load(&d, "({f(v){globalThis.counter=(globalThis.counter||0)+1;Array.prototype.x=9;return BigInt(counter);}})", ValueType::Int64);
    let start = Instant::now();
    for _ in 0..500 {
        assert_eq!(f.invoke(&[Scalar::Int64(0)]).unwrap(), Scalar::Int64(1));
    }
    eprintln!("scripts 500 fresh-context IPC calls: {:?}", start.elapsed());
    let source = "({f(v){return v;}})";
    for n in 2..=4 {
        let hash = m
            .install(
                manifest(source, &format!("v{n}"), ValueType::Int64),
                source.as_bytes(),
            )
            .unwrap()
            .manifest_sha256;
        m.enable(&hash, &hash).unwrap();
    }
    let fifth = m
        .install(manifest(source, "v5", ValueType::Int64), source.as_bytes())
        .unwrap()
        .manifest_sha256;
    assert_eq!(
        m.enable(&fifth, &fifth).unwrap_err().code,
        ErrorCode::ResourceExhausted
    );
    drop(f);
    m.disable(&h).unwrap();
    m.enable(&fifth, &fifth).unwrap();
    drop(m);
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn scripts_invalid_source_hash_target_and_initialization() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let m = Manager::open_with_scripts(&d.0.join("plugins"), false, Some(worker(&d))).unwrap();
    for (i, source) in ["syntax !!", "({f:42})", "for(;;){}", "({get f(){throw 1}})"]
        .iter()
        .enumerate()
    {
        let hash = m
            .install(
                manifest(source, &format!("v{i}"), ValueType::Int64),
                source.as_bytes(),
            )
            .unwrap()
            .manifest_sha256;
        assert!(m.enable(&hash, &hash).is_err());
        m.uninstall(&hash).unwrap();
        assert_eq!(script::worker_count(), 0);
    }
    let source = "({f(v){return v;}})";
    let mut man = manifest(source, "v1", ValueType::Int64);
    assert!(man.check_artifact(b"changed").is_err());
    man.target = "javascript-quickjs-latest".into();
    assert!(man.validate().is_err());
    let large = "x".repeat(MAX_SCRIPT + 1);
    assert!(m
        .install(manifest(&large, "v2", ValueType::Int64), large.as_bytes())
        .is_err());
}

#[test]
fn scripts_worker_path_validation_and_failed_handshake_release_capacity() {
    use std::os::unix::fs::PermissionsExt;
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let link = d.0.join("worker-link");
    std::os::unix::fs::symlink(worker(&d), &link).unwrap();
    assert!(script::validate_worker(&link).is_err());
    assert!(script::validate_worker(std::path::Path::new("relative-worker")).is_err());
    let bad = d.0.join("worker");
    std::fs::write(&bad, b"#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(script::validate_worker(&bad).is_err());
    std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o700)).unwrap();
    let m = Manager::open_with_scripts(&d.0.join("plugins"), false, Some(bad)).unwrap();
    let source = "({f(v){return v;}})";
    let hash = m
        .install(manifest(source, "v1", ValueType::Int64), source.as_bytes())
        .unwrap()
        .manifest_sha256;
    for _ in 0..6 {
        assert!(m.enable(&hash, &hash).is_err());
        assert_eq!(script::worker_count(), 0);
    }
    m.uninstall(&hash).unwrap();
}

#[test]
fn scripts_running_kernel_cancellation_reaps_worker_and_refunds_job() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m, h, f) = load(
        &d,
        "({f(v){/(a+)+$/.test('a'.repeat(30)+'!');return v;}})",
        ValueType::Int64,
    );
    drop(f);
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.plugins = Some(m.clone());
    catalog.insert(
        "s",
        sparrow_model::Schema::new(
            1,
            vec![sparrow_model::Field::new(
                1,
                "value",
                sparrow_model::DataType::Int64,
                false,
            )],
        )
        .unwrap(),
    );
    let sql = format!("SELECT plugin_call('script_test','v1','{h}','f',value) AS value FROM s");
    let plan = sparrow_plan::physicalize(
        &sparrow_sql::bind_sql(&sql, &catalog, 1.into(), 1.into()).unwrap(),
        &Default::default(),
    );
    let kernel = sparrow_runtime::Kernel::new(Default::default()).unwrap();
    let job = kernel
        .submit(sparrow_runtime::JobRequest::new(
            plan,
            vec![sparrow_model::Row {
                values: vec![Scalar::Int64(1)],
            }],
            sparrow_runtime::SharedCapture::new(),
        ))
        .unwrap();
    let owner = job.memory_owner();
    let start = Instant::now();
    while m.list().unwrap()[0].script_worker_state != Some("busy") {
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "worker did not start"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let result = kernel.block_on(job.stop());
    assert!(
        result.is_ok()
            || result
                .as_ref()
                .is_err_and(|e| e.code == ErrorCode::Cancelled),
        "{result:?}"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(owner.usage().physical_bytes, 0);
    assert_eq!(kernel.live_tasks(), 0);
    assert_eq!(m.list().unwrap()[0].script_worker_state, Some("failed"));
    m.disable(&h).unwrap();
    m.uninstall(&h).unwrap();
    assert_eq!(script::worker_count(), 0);
}

#[test]
fn scripts_payload_cap_fresh_builtins_and_no_async_jobs() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m, h, f) = load(
        &d,
        "({f(v){JSON.parse(JSON.stringify(v));return v;}})",
        ValueType::Utf8,
    );
    let large = Scalar::utf8("x".repeat(65536));
    assert_eq!(f.invoke(std::slice::from_ref(&large)).unwrap(), large);
    assert!(f.invoke(&[Scalar::utf8("x".repeat(65537))]).is_err());
    drop(f);
    m.disable(&h).unwrap();
    let source = "({f(a,b){return a+b;}})";
    let mut man = manifest(source, "v2", ValueType::Utf8);
    man.functions[0].inputs.push(ValueType::Utf8);
    let id = m.install(man, source.as_bytes()).unwrap().manifest_sha256;
    m.enable(&id, &id).unwrap();
    assert!(m
        .resolve("script_test", "v2", &id, "f")
        .unwrap()
        .invoke(&[
            Scalar::utf8("x".repeat(40000)),
            Scalar::utf8("x".repeat(30000))
        ])
        .is_err());
    m.disable(&id).unwrap();
    let source = "({f(v){let x=v;Promise.resolve().then(()=>{x=999n});return x;}})";
    let id = m
        .install(manifest(source, "v3", ValueType::Int64), source.as_bytes())
        .unwrap()
        .manifest_sha256;
    m.enable(&id, &id).unwrap();
    let f = m.resolve("script_test", "v3", &id, "f").unwrap();
    for _ in 0..10 {
        assert_eq!(f.invoke(&[Scalar::Int64(7)]).unwrap(), Scalar::Int64(7));
    }
}

#[test]
fn scripts_parallel_packages_and_shared_function_are_isolated() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m, h, f) = load(&d, "({f(v){return v*2n;}})", ValueType::Int64);
    let threads: Vec<_> = (0..4)
        .map(|n| {
            let f = f.clone();
            std::thread::spawn(move || {
                for i in 0..20 {
                    assert_eq!(
                        f.invoke(&[Scalar::Int64(n * 100 + i)]).unwrap(),
                        Scalar::Int64(2 * (n * 100 + i))
                    );
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let bad = "({f(v){for(let i=0;i<9999;i++)for(let j=0;j<9999;j++)Math.sqrt(j);return v;}})";
    let other = m
        .install(manifest(bad, "v2", ValueType::Int64), bad.as_bytes())
        .unwrap()
        .manifest_sha256;
    m.enable(&other, &other).unwrap();
    let broken = m.resolve("script_test", "v2", &other, "f").unwrap();
    assert!(broken.invoke(&[Scalar::Int64(1)]).is_err());
    assert_eq!(f.invoke(&[Scalar::Int64(21)]).unwrap(), Scalar::Int64(42));
    drop(broken);
    drop(f);
    m.disable(&other).unwrap();
    m.disable(&h).unwrap();
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn scripts_sql_graph_kernel_execution_and_fresh_only() {
    let _serial = SERIAL.lock().unwrap();
    let d = dir();
    let (m, h, f) = load(&d, "({f(v){return v*2n;}})", ValueType::Int64);
    drop(f);
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.plugins = Some(m.clone());
    let schema = sparrow_model::Schema::new(
        1,
        vec![sparrow_model::Field::new(
            1,
            "value",
            sparrow_model::DataType::Int64,
            true,
        )],
    )
    .unwrap();
    catalog.insert("s", schema);
    let sql = format!("SELECT plugin_call('script_test','v1','{h}','f',value) AS doubled FROM s");
    let logical = sparrow_sql::bind_sql(&sql, &catalog, 1.into(), 1.into()).unwrap();
    let plan = sparrow_plan::physicalize(&logical, &Default::default());
    drop(logical);
    assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
    let kernel = sparrow_runtime::Kernel::new(Default::default()).unwrap();
    let out = sparrow_runtime::SharedCapture::new();
    kernel
        .run(sparrow_runtime::JobRequest::new(
            plan.clone(),
            vec![sparrow_model::Row {
                values: vec![Scalar::Int64(21)],
            }],
            out.clone(),
        ))
        .unwrap();
    assert_eq!(out.rows(), vec![vec![Scalar::Int64(42)]]);
    let raw = serde_json::json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2]},{"id":2,"kind":"project","exprs":[{"alias":"doubled","expr":{"k":"call","name":"plugin_call","args":[{"k":"lit","value":{"t":"utf8","v":"script_test"}},{"k":"lit","value":{"t":"utf8","v":"v1"}},{"k":"lit","value":{"t":"utf8","v":h}},{"k":"lit","value":{"t":"utf8","v":"f"}},{"k":"col","name":"value"}]}}],"out":[3]},{"id":3,"kind":"capture_sink"}]});
    let graph = sparrow_plan::GraphSpec::from_json(&raw.to_string()).unwrap();
    let graph = sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &catalog).unwrap(),
        &Default::default(),
    );
    let out = sparrow_runtime::SharedCapture::new();
    kernel
        .run(sparrow_runtime::JobRequest::new(
            graph,
            vec![sparrow_model::Row {
                values: vec![Scalar::Int64(-4)],
            }],
            out.clone(),
        ))
        .unwrap();
    assert_eq!(out.rows(), vec![vec![Scalar::Int64(-8)]]);
    assert!(m.disable(&h).is_err());
    drop(plan);
    m.disable(&h).unwrap();
    assert_eq!(kernel.live_tasks(), 0);
}

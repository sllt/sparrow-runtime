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
    let path = std::env::temp_dir().join(format!(
        "sparrow-wasm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    Dir(path)
}
fn manager(d: &Dir) -> Arc<Manager> {
    use std::os::unix::fs::PermissionsExt;
    let exe = d.0.join("worker");
    let sibling = std::env::current_exe()
        .unwrap()
        .with_file_name("sparrow-wasm-worker");
    let input = std::env::var_os("SPARROW_TEST_WASM_WORKER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            if sibling.is_file() {
                sibling
            } else {
                env!("CARGO_BIN_EXE_sparrow-wasm-worker").into()
            }
        });
    std::fs::copy(input, &exe).unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o700)).unwrap();
    Manager::open_with_workers(&d.0.join("plugins"), false, None, Some(exe)).unwrap()
}
fn manifest(bytes: &[u8], version: &str, ty: ValueType) -> Manifest {
    Manifest {
        format: 1,
        name: "wasm_test".into(),
        version: version.into(),
        kind: "wasm_scalar".into(),
        abi: 1,
        semantics: 1,
        target: WASM_TARGET.into(),
        artifact_sha256: sha256(bytes),
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
fn wat(body: &str) -> Vec<u8> {
    wat::parse_str(format!(
        "(module (memory (export \"memory\") 3 256)
      (global $n (mut i64) (i64.const 0))
      (func (export \"sparrow_wasm_abi_v1\") (result i32) i32.const 1)
      (func (export \"sparrow_wasm_buffer_v1\") (result i32) i32.const 0)
      (func (export \"sparrow_wasm_call_v1\") (param $id i32) (param $in i32) (param $count i32)
      (param $out i32) (param $buf i32) (param $cap i32) (result i32) {body}))"
    ))
    .unwrap()
}
const IDENTITY:&str="(memory.copy (local.get $out) (local.get $in) (i32.const 24))
    (if (i32.or (i32.eq (i32.load (local.get $in)) (i32.const 5)) (i32.eq (i32.load (local.get $in)) (i32.const 6)))
    (then (memory.copy (local.get $buf) (i32.load offset=16 (local.get $in)) (i32.load offset=4 (local.get $in)))
    (i32.store offset=16 (local.get $out) (local.get $buf)))) i32.const 0";
fn load(m: &Manager, bytes: &[u8], version: &str, ty: ValueType) -> (String, Arc<Function>) {
    let hash = m
        .install(manifest(bytes, version, ty), bytes)
        .unwrap()
        .manifest_sha256;
    m.enable(&hash, &hash).unwrap();
    let f = m.resolve("wasm_test", version, &hash, "f").unwrap();
    (hash, f)
}
#[test]
fn wasm_all_scalar_tags_exact_bits_payload_and_null() {
    let _g = SERIAL.lock().unwrap();
    let d = dir();
    let m = manager(&d);
    let bytes = wat(IDENTITY);
    for (i, (ty, value)) in [
        (ValueType::Bool, Scalar::Bool(true)),
        (ValueType::Int64, Scalar::Int64(i64::MIN)),
        (ValueType::UInt64, Scalar::UInt64(u64::MAX)),
        (ValueType::Float64, Scalar::Float64(-0.5)),
        (
            ValueType::TimestampMicrosUtc,
            Scalar::TimestampMicrosUTC(i64::MAX),
        ),
        (ValueType::Utf8, Scalar::utf8("中\0文😀")),
        (ValueType::Bytes, Scalar::bytes([0, 128, 255])),
    ]
    .into_iter()
    .enumerate()
    {
        let (h, f) = load(&m, &bytes, &format!("v{i}"), ty);
        assert_eq!(f.invoke(&[value.clone()]).unwrap(), value);
        assert_eq!(f.invoke(&[Scalar::Null]).unwrap(), Scalar::Null);
        assert!(f.is_preemptible());
        assert!(!f.is_script());
        let info = m
            .list()
            .unwrap()
            .into_iter()
            .find(|p| p.manifest_sha256 == h)
            .unwrap();
        assert_eq!(info.wasm_module.unwrap().artifact_sha256, sha256(&bytes));
        assert!(info.hot_unload);
        assert!(m.disable(&h).is_err());
        drop(f);
        m.disable(&h).unwrap();
        m.uninstall(&h).unwrap();
    }
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn wasm_fuel_memory_traps_bad_output_and_fresh_instance() {
    let _g = SERIAL.lock().unwrap();
    let d = dir();
    let m = manager(&d);
    for (i, body) in [
        "(loop $l (br $l)) i32.const 0",
        "unreachable",
        "(i32.store (local.get $out) (i32.const 99)) i32.const 0",
        "(i32.store offset=20 (local.get $out) (i32.const 1)) i32.const 0",
        "(i32.store (i32.const -1) (i32.const 1)) i32.const 0",
        "i32.const 7",
    ]
    .into_iter()
    .enumerate()
    {
        let (h, f) = load(&m, &wat(body), &format!("bad{i}"), ValueType::Int64);
        let start = Instant::now();
        assert!(f.invoke(&[Scalar::Int64(1)]).is_err(), "{body}");
        assert!(start.elapsed() < Duration::from_secs(1));
        drop(f);
        m.disable(&h).unwrap();
        m.uninstall(&h).unwrap();
    }
    // Core WASM can refuse growth with -1 before consulting the host limiter.
    let refused=wat("(i32.store (local.get $out) (i32.const 2))
        (i64.store offset=8 (local.get $out) (i64.extend_i32_s (memory.grow (i32.const 300)))) i32.const 0");
    let (h, f) = load(&m, &refused, "grow", ValueType::Int64);
    assert_eq!(f.invoke(&[Scalar::Int64(1)]).unwrap(), Scalar::Int64(-1));
    drop(f);
    m.disable(&h).unwrap();
    m.uninstall(&h).unwrap();
    let bytes=wat("(global.set $n (i64.add (global.get $n) (i64.const 1)))
        (i32.store (local.get $out) (i32.const 2)) (i64.store offset=8 (local.get $out) (global.get $n)) i32.const 0");
    let (h, f) = load(&m, &bytes, "fresh", ValueType::Int64);
    for _ in 0..500 {
        assert_eq!(f.invoke(&[Scalar::Int64(1)]).unwrap(), Scalar::Int64(1));
    }
    drop(f);
    m.disable(&h).unwrap();
    assert_eq!(script::worker_count(), 0);
}
#[test]
fn wasm_module_validation_no_imports_start_or_excess_memory() {
    let _g = SERIAL.lock().unwrap();
    let d = dir();
    let m = manager(&d);
    for (i, source) in [
        "(module)",
        "(module (import \"wasi_snapshot_preview1\" \"fd_write\" (func)))",
        "(module (func $start) (start $start))",
        "(module (memory 65535))",
    ]
    .into_iter()
    .enumerate()
    {
        let bytes = wat::parse_str(source).unwrap();
        let h = m
            .install(manifest(&bytes, &format!("v{i}"), ValueType::Int64), &bytes)
            .unwrap()
            .manifest_sha256;
        assert!(m.enable(&h, &h).is_err());
        m.uninstall(&h).unwrap();
        assert_eq!(script::worker_count(), 0);
    }
    let bytes = b"\0asm\x01\0\0\0broken";
    let h = m
        .install(manifest(bytes, "broken", ValueType::Int64), bytes)
        .unwrap()
        .manifest_sha256;
    assert!(m.enable(&h, &h).is_err());
}
#[test]
fn wasm_sql_finite_budget_checkpoint_rejection_and_restart() {
    let _g = SERIAL.lock().unwrap();
    let d = dir();
    let m = manager(&d);
    let bytes = wat(IDENTITY);
    let (h, f) = load(&m, &bytes, "v1", ValueType::Int64);
    drop(f);
    let mut catalog = sparrow_plan::Catalog::new();
    catalog.plugins = Some(m.clone());
    catalog.insert(
        "s",
        sparrow_model::Schema::new(
            1,
            vec![sparrow_model::Field::new(
                1,
                "v",
                sparrow_model::DataType::Int64,
                false,
            )],
        )
        .unwrap(),
    );
    let sql = format!("SELECT plugin_call('wasm_test','v1','{h}','f',v) AS v FROM s");
    let plan = sparrow_plan::physicalize(
        &sparrow_sql::bind_sql(&sql, &catalog, 1.into(), 1.into()).unwrap(),
        &Default::default(),
    );
    assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
    let source = plan
        .stages
        .iter()
        .find_map(|s| match s {
            sparrow_plan::PhysicalStage::MemorySource { operator, .. } => Some(*operator),
            _ => None,
        })
        .unwrap();
    for work in [100_000, 15_000] {
        let result = sparrow_runtime::finite::execute(
            plan.clone(),
            [(
                source,
                vec![
                    sparrow_model::Row {
                        values: vec![Scalar::Int64(42)],
                    };
                    2
                ],
            )]
            .into(),
            sparrow_runtime::finite::FiniteLimits {
                work_units: work,
                ..Default::default()
            },
            tokio_util::sync::CancellationToken::new(),
        );
        if work == 15_000 {
            let error = result.err().unwrap();
            assert_eq!(error.code, ErrorCode::ResourceExhausted);
            assert!(error.message.contains("aggregate WebAssembly"), "{error:?}");
        } else {
            let result = result.unwrap();
            assert_eq!(result.batches[0].rows()[0].values, vec![Scalar::Int64(42)]);
        }
    }
    let graph = serde_json::json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
        {"id":1,"kind":"memory_source","table":"s","out":[2]},
        {"id":2,"kind":"project","exprs":[{"alias":"v","expr":{"k":"call","name":"plugin_call","args":[
            {"k":"lit","value":{"t":"utf8","v":"wasm_test"}}, {"k":"lit","value":{"t":"utf8","v":"v1"}},
            {"k":"lit","value":{"t":"utf8","v":h}}, {"k":"lit","value":{"t":"utf8","v":"f"}}, {"k":"col","name":"v"}]}}],"out":[3]},
        {"id":3,"kind":"capture_sink"}]});
    let spec = sparrow_plan::GraphSpec::from_json(&graph.to_string()).unwrap();
    let graph = sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&spec, &catalog).unwrap(),
        &Default::default(),
    );
    let kernel = sparrow_runtime::Kernel::new(Default::default()).unwrap();
    let capture = sparrow_runtime::SharedCapture::new();
    kernel
        .run(sparrow_runtime::JobRequest::new(
            graph,
            vec![sparrow_model::Row {
                values: vec![Scalar::Int64(7)],
            }],
            capture.clone(),
        ))
        .unwrap();
    assert_eq!(capture.rows(), vec![vec![Scalar::Int64(7)]]);
    drop(plan);
    drop(catalog);
    drop(m);
    let safe = Manager::open(&d.0.join("plugins"), false).unwrap();
    assert!(!safe.list().unwrap()[0].enabled);
    drop(safe);
    let m = manager(&d);
    assert!(m.list().unwrap()[0].enabled);
    m.disable(&h).unwrap();
    m.uninstall(&h).unwrap();
    assert_eq!(script::worker_count(), 0);
}

#[test]
fn wasm_payload_bound_output_contract_and_cancelled_call() {
    let _g = SERIAL.lock().unwrap();
    let d = dir();
    let m = manager(&d);
    for (i, ty) in [ValueType::Utf8, ValueType::Bytes].into_iter().enumerate() {
        let (h, f) = load(&m, &wat(IDENTITY), &format!("v{i}"), ty);
        let value = if ty == ValueType::Utf8 {
            Scalar::utf8("x".repeat(MAX_VALUE))
        } else {
            Scalar::bytes(vec![255; MAX_VALUE])
        };
        assert_eq!(f.invoke(std::slice::from_ref(&value)).unwrap(), value);
        let over = if ty == ValueType::Utf8 {
            Scalar::utf8("x".repeat(MAX_VALUE + 1))
        } else {
            Scalar::bytes(vec![255; MAX_VALUE + 1])
        };
        assert!(f.invoke(&[over]).is_err());
        drop(f);
        m.disable(&h).unwrap();
        m.uninstall(&h).unwrap();
    }
    for (i,body) in [
        "(i32.store (local.get $out) (i32.const 5)) (i32.store offset=4 (local.get $out) (i32.const 65537)) i32.const 0",
        "(i32.store (local.get $out) (i32.const 5)) (i32.store offset=16 (local.get $out) (i32.const -1)) i32.const 0",
        "(i32.store (local.get $out) (i32.const 5)) (i32.store offset=4 (local.get $out) (i32.const 1)) (i32.store offset=16 (local.get $out) (local.get $buf)) (i32.store8 (local.get $buf) (i32.const 255)) i32.const 0"
    ].into_iter().enumerate() {
        let (h,f)=load(&m,&wat(body),&format!("bad{i}"),ValueType::Utf8);
        assert!(f.invoke(&[Scalar::utf8("")]).is_err());drop(f);m.disable(&h).unwrap();m.uninstall(&h).unwrap();
    }
    let (h, f) = load(&m, &wat(IDENTITY), "cancel", ValueType::Int64);
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let e = rt
        .block_on(script::scope(token, async {
            f.invoke(&[Scalar::Int64(1)])
        }))
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Cancelled);
    assert_eq!(f.invoke(&[Scalar::Int64(2)]).unwrap(), Scalar::Int64(2));
    drop(f);
    m.disable(&h).unwrap();
}

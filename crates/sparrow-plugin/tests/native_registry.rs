#![cfg(all(target_os = "linux", target_env = "gnu"))]
use sparrow_model::{DataType, Scalar};
use sparrow_plugin::*;
use std::{path::PathBuf, sync::OnceLock};
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn dir() -> Dir {
    let d = std::env::temp_dir().join(format!(
        "sparrow-plugin-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&d).unwrap();
    Dir(d)
}
fn artifact() -> &'static Vec<u8> {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| {
        let d = dir();
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        assert!(std::process::Command::new("cc")
            .args([
                "-std=c11",
                "-O2",
                "-fPIC",
                "-shared",
                "-DSPARROW_TEST_FAILURES"
            ])
            .arg(format!("-I{}", root.join("sdk/native").display()))
            .arg(root.join("examples/plugins/native_math.c"))
            .arg("-o")
            .arg(d.0.join("test.so"))
            .status()
            .unwrap()
            .success());
        std::fs::read(d.0.join("test.so")).unwrap()
    })
}
fn manifest(version: &str) -> Manifest {
    Manifest {
        format: 1,
        name: "native_test".into(),
        version: version.into(),
        kind: "native_scalar".into(),
        abi: 1,
        semantics: 1,
        target: host_target().into(),
        artifact_sha256: sha256(artifact()),
        deterministic: true,
        thread_safe: true,
        null_policy: "propagate".into(),
        functions: vec![
            FunctionDef {
                name: "double".into(),
                id: 1,
                inputs: vec![ValueType::Int64],
                output: ValueType::Int64,
                max_output_bytes: 1,
            },
            FunctionDef {
                name: "upper".into(),
                id: 2,
                inputs: vec![ValueType::Utf8],
                output: ValueType::Utf8,
                max_output_bytes: 128,
            },
            FunctionDef {
                name: "bad_len".into(),
                id: 3,
                inputs: vec![ValueType::Int64],
                output: ValueType::Utf8,
                max_output_bytes: 128,
            },
            FunctionDef {
                name: "bad_bool".into(),
                id: 4,
                inputs: vec![ValueType::Int64],
                output: ValueType::Bool,
                max_output_bytes: 1,
            },
            FunctionDef {
                name: "bad_float".into(),
                id: 5,
                inputs: vec![ValueType::Int64],
                output: ValueType::Float64,
                max_output_bytes: 1,
            },
            FunctionDef {
                name: "fail".into(),
                id: 6,
                inputs: vec![ValueType::Int64],
                output: ValueType::Int64,
                max_output_bytes: 1,
            },
        ],
    }
}
#[test]
fn plugins_native_identity_pin_disable_restart_and_uninstall() {
    let d = dir();
    let root = d.0.join("packages");
    let manager = Manager::open(&root, true).unwrap();
    assert!(Manager::open(&root, true).is_err());
    let info = manager.install(manifest("v1"), artifact()).unwrap();
    let id = info.manifest_sha256;
    assert!(!info.enabled);
    assert!(manager.resolve("native_test", "v1", &id, "double").is_err());
    assert!(manager.enable(&id, &"0".repeat(64)).is_err());
    manager.enable(&id, &id).unwrap();
    let function = manager.resolve("native_test", "v1", &id, "double").unwrap();
    assert_eq!(
        function.signature(&[DataType::Int64]).unwrap(),
        DataType::Int64
    );
    assert_eq!(
        function.invoke(&[Scalar::Int64(21)]).unwrap(),
        Scalar::Int64(42)
    );
    assert_eq!(function.invoke(&[Scalar::Null]).unwrap(), Scalar::Null);
    assert!(function.invoke(&[Scalar::Int64(i64::MAX)]).is_err());
    assert!(function.signature(&[DataType::Utf8]).is_err());
    assert!(manager.disable(&id).is_err());
    assert!(manager.uninstall(&id).is_err());
    drop(function);
    manager.disable(&id).unwrap();
    assert!(manager.uninstall(&id).is_err());
    drop(manager);
    let manager = Manager::open(&root, false).unwrap();
    manager.uninstall(&id).unwrap();
    assert!(manager.list().unwrap().is_empty());
}
#[test]
fn plugins_native_reopen_upgrade_and_explicit_old_hash_rollback() {
    let d = dir();
    let root = d.0.join("packages");
    let manager = Manager::open(&root, true).unwrap();
    let old = manager
        .install(manifest("v1"), artifact())
        .unwrap()
        .manifest_sha256;
    manager.enable(&old, &old).unwrap();
    let new = manager
        .install(manifest("v2"), artifact())
        .unwrap()
        .manifest_sha256;
    manager.enable(&new, &new).unwrap();
    let old_function = manager
        .resolve("native_test", "v1", &old, "double")
        .unwrap();
    assert!(manager
        .resolve("native_test", "v2", &old, "double")
        .is_err());
    drop(old_function);
    drop(manager);
    let manager = Manager::open(&root, true).unwrap();
    assert!(manager.list().unwrap().iter().all(|p| p.enabled));
    for (version, id) in [("v1", &old), ("v2", &new)] {
        assert_eq!(
            manager
                .resolve("native_test", version, id, "double")
                .unwrap()
                .invoke(&[Scalar::Int64(3)])
                .unwrap(),
            Scalar::Int64(6)
        );
    }
}
#[test]
fn plugins_native_invalid_outputs_and_trust_gate() {
    let d = dir();
    let root = d.0.join("packages");
    let manager = Manager::open(&root, false).unwrap();
    let id = manager
        .install(manifest("v1"), artifact())
        .unwrap()
        .manifest_sha256;
    assert!(manager.enable(&id, &id).is_err());
    drop(manager);
    let manager = Manager::open(&root, true).unwrap();
    manager.enable(&id, &id).unwrap();
    for name in ["bad_len", "bad_bool", "bad_float", "fail"] {
        assert!(manager
            .resolve("native_test", "v1", &id, name)
            .unwrap()
            .invoke(&[Scalar::Int64(1)])
            .is_err());
    }
    assert_eq!(
        manager
            .resolve("native_test", "v1", &id, "upper")
            .unwrap()
            .invoke(&[Scalar::utf8("aBc🙂")])
            .unwrap(),
        Scalar::utf8("ABC🙂")
    );
    assert_eq!(manager.list().unwrap()[0].pins, 0);
}
#[test]
fn plugins_manifest_corruption_versions_and_symlinks_fail_closed() {
    let d = dir();
    let root = d.0.join("packages");
    let manager = Manager::open(&root, false).unwrap();
    let m = manifest("v1");
    let mut broken = artifact().clone();
    broken[10] ^= 1;
    assert!(manager.install(m.clone(), &broken).is_err());
    for bad in [
        "script_scalar",
        "wasm_scalar",
        "native_source",
        "native_sink",
    ] {
        let mut bad_m = m.clone();
        bad_m.kind = bad.into();
        assert!(manager.install(bad_m, artifact()).is_err());
    }
    let mut wrong = m.clone();
    wrong.abi = 2;
    assert!(manager.install(wrong, artifact()).is_err());
    let mut wrong = m.clone();
    wrong.functions[0].inputs = vec![ValueType::Int64; 9];
    assert!(manager.install(wrong, artifact()).is_err());
    let id = manager
        .install(m.clone(), artifact())
        .unwrap()
        .manifest_sha256;
    let mut changed = m;
    changed.functions[0].max_output_bytes = 2;
    assert!(manager.install(changed, artifact()).is_err());
    drop(manager);
    std::fs::remove_file(root.join(&id).join("artifact.so")).unwrap();
    std::os::unix::fs::symlink("/dev/null", root.join(&id).join("artifact.so")).unwrap();
    assert!(Manager::open(&root, false).is_err());
}
#[test]
fn plugins_wrong_exported_abi_is_resident_rejected_and_never_enabled() {
    let d = dir();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let library = d.0.join("wrong.so");
    assert!(std::process::Command::new("cc")
        .args([
            "-std=c11",
            "-O2",
            "-fPIC",
            "-shared",
            "-DSPARROW_TEST_ABI=2"
        ])
        .arg(format!("-I{}", root.join("sdk/native").display()))
        .arg(root.join("examples/plugins/native_math.c"))
        .arg("-o")
        .arg(&library)
        .status()
        .unwrap()
        .success());
    let bytes = std::fs::read(library).unwrap();
    let mut m = manifest("wrong");
    m.artifact_sha256 = sha256(&bytes);
    let manager = Manager::open(&d.0.join("packages"), true).unwrap();
    let id = manager.install(m, &bytes).unwrap().manifest_sha256;
    assert!(manager.enable(&id, &id).is_err());
    let info = manager.list().unwrap().pop().unwrap();
    assert!(!info.enabled);
    assert!(info.resident);
    assert!(manager.enable(&id, &id).is_err());
    assert!(manager.uninstall(&id).is_err());
}
#[test]
fn plugins_package_bound_and_orphan_staging_cleanup() {
    let d = dir();
    let root = d.0.join("packages");
    let manager = Manager::open(&root, false).unwrap();
    for i in 0..MAX_PACKAGES {
        manager
            .install(manifest(&format!("v{i}")), artifact())
            .unwrap();
    }
    assert!(manager.install(manifest("extra"), artifact()).is_err());
    drop(manager);
    let staging = root.join(".staging-old");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("partial"), b"x").unwrap();
    let manager = Manager::open(&root, false).unwrap();
    assert!(!staging.exists());
    assert_eq!(manager.list().unwrap().len(), MAX_PACKAGES);
}

#[test]
fn plugins_all_scalar_tags_roundtrip_without_foreign_ownership() {
    let cases = [
        (ValueType::Bool, Scalar::Bool(true)),
        (ValueType::Int64, Scalar::Int64(i64::MIN)),
        (ValueType::UInt64, Scalar::UInt64(u64::MAX)),
        (ValueType::Float64, Scalar::Float64(0.125)),
        (ValueType::Utf8, Scalar::utf8("边缘🙂")),
        (ValueType::Bytes, Scalar::bytes([0u8, 255, 7])),
        (
            ValueType::TimestampMicrosUtc,
            Scalar::TimestampMicrosUTC(-1),
        ),
    ];
    let d = dir();
    let manager = Manager::open(&d.0.join("packages"), true).unwrap();
    let mut m = manifest("tags");
    m.functions = cases
        .iter()
        .enumerate()
        .map(|(i, (ty, _))| FunctionDef {
            name: format!("echo{i}"),
            id: 7 + i as u32,
            inputs: vec![*ty],
            output: *ty,
            max_output_bytes: 128,
        })
        .collect();
    let id = manager.install(m, artifact()).unwrap().manifest_sha256;
    manager.enable(&id, &id).unwrap();
    for (i, (_, value)) in cases.iter().enumerate() {
        let function = manager
            .resolve("native_test", "tags", &id, &format!("echo{i}"))
            .unwrap();
        assert_eq!(
            function.invoke(std::slice::from_ref(value)).unwrap(),
            *value
        );
        assert_eq!(function.invoke(&[Scalar::Null]).unwrap(), Scalar::Null);
        assert!(function.invoke(&[]).is_err());
    }
    assert!(manager
        .resolve("native_test", "tags", &id, "echo3")
        .unwrap()
        .invoke(&[Scalar::Float64(f64::NAN)])
        .is_err());
}

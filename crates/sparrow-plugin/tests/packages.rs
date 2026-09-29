use base64::{engine::general_purpose::STANDARD, Engine};
use ring::signature::{Ed25519KeyPair, KeyPair};
use sparrow_plugin::{trust::*, *};
use std::path::PathBuf;
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn dir() -> Dir {
    let path = std::env::temp_dir().join(format!(
        "sparrow-packages-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    Dir(path)
}
const SOURCE: &[u8] = b"({f(v){return v;}})";
fn manifest(name: &str, version: &str) -> Manifest {
    Manifest {
        format: 1,
        name: name.into(),
        version: version.into(),
        kind: "javascript_scalar".into(),
        abi: 1,
        semantics: 1,
        target: JS_TARGET.into(),
        artifact_sha256: sha256(SOURCE),
        deterministic: true,
        thread_safe: true,
        null_policy: "propagate".into(),
        functions: vec![FunctionDef {
            name: "f".into(),
            id: 1,
            inputs: vec![ValueType::Int64],
            output: ValueType::Int64,
            max_output_bytes: 8,
        }],
        package: None,
    }
}
fn key() -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[17; 32]).unwrap()
}
fn policy() -> TrustPolicy {
    TrustPolicy {
        format: 1,
        require_signed: true,
        publishers: vec![Publisher {
            id: "publisher".into(),
            public_key_base64: STANDARD.encode(key().public_key().as_ref()),
            packages: vec!["*".into()],
            kinds: vec!["javascript_scalar".into()],
            revoked: false,
        }],
    }
}
fn signature(manifest: &Manifest) -> Signature {
    Signature {
        algorithm: "ed25519".into(),
        key_id: "publisher".into(),
        signature_base64: STANDARD.encode(
            key()
                .sign(&signing_message(manifest, "publisher").unwrap())
                .as_ref(),
        ),
    }
}
fn reference(m: &Manifest) -> PackageReference {
    PackageReference {
        name: m.name.clone(),
        version: m.version.clone(),
        manifest_sha256: m.identity().unwrap(),
    }
}
fn requires(mut m: Manifest, deps: Vec<PackageReference>) -> Manifest {
    m.format = 2;
    m.package = Some(PackageMetadata {
        dependencies: deps,
        platform: vec!["sparrow_abi_v1".into(), "quickjs_ng_0_16_2".into()],
    });
    m
}
#[test]
fn packages_signatures_tamper_revocation_scope_and_legacy_identity() {
    let m = manifest("pkg", "v1");
    let original = serde_json::to_value(&m).unwrap();
    assert!(original.get("package").is_none());
    assert_eq!(m.identity().unwrap(), sha256(&m.bytes().unwrap()));
    let sig = signature(&m);
    let p = policy();
    p.verify(&m, Some(&sig)).unwrap();
    assert!(p.verify(&m, None).is_err());
    let mut changed = m.clone();
    changed.version = "v2".into();
    assert!(p.verify(&changed, Some(&sig)).is_err());
    let mut bad = sig.clone();
    bad.key_id = "other".into();
    assert!(p.verify(&m, Some(&bad)).is_err());
    bad = sig.clone();
    bad.algorithm = "rsa".into();
    assert!(p.verify(&m, Some(&bad)).is_err());
    bad = sig.clone();
    bad.signature_base64 = STANDARD.encode([0; 64]);
    assert!(p.verify(&m, Some(&bad)).is_err());
    let mut revoked = p.clone();
    revoked.publishers[0].revoked = true;
    assert!(revoked.verify(&m, Some(&sig)).is_err());
    let mut restricted = p.clone();
    restricted.publishers[0].packages = vec!["other".into()];
    assert!(restricted.verify(&m, Some(&sig)).is_err());
    let mut other_kind = p.clone();
    other_kind.publishers[0].kinds = vec!["native_scalar".into()];
    assert!(other_kind.verify(&m, Some(&sig)).is_err());
    let mut duplicate = p;
    duplicate.publishers.push(duplicate.publishers[0].clone());
    assert!(duplicate.validate().is_err());
}
#[test]
fn packages_signed_install_reopen_attest_and_provenance_are_explicit() {
    let d = dir();
    let p = policy();
    let m = manifest("pkg", "v1");
    let sig = signature(&m);
    let manager = Manager::open_with_policy(&d.0.join("p"), false, None, None, p.clone()).unwrap();
    assert!(manager.install(m.clone(), SOURCE).is_err());
    let info = manager
        .install_signed(m.clone(), SOURCE, Some(sig.clone()))
        .unwrap();
    let digest = info.manifest_sha256;
    assert!(info.signature_verified);
    assert_eq!(info.publisher.as_deref(), Some("publisher"));
    assert!(manager
        .install_signed(m.clone(), b"changed", Some(sig.clone()))
        .is_err());
    drop(manager);
    let manager = Manager::open_with_policy(&d.0.join("p"), false, None, None, p).unwrap();
    assert!(manager.list().unwrap()[0].signature_verified);
    manager.attest(&digest, sig).unwrap();
    manager.uninstall(&digest).unwrap();
    drop(manager);
    let manager = Manager::open(&d.0.join("p"), false).unwrap();
    let legacy = manager.install(m.clone(), SOURCE).unwrap();
    assert!(!legacy.signature_verified);
    drop(manager);
    let manager = Manager::open_with_policy(&d.0.join("p"), false, None, None, policy()).unwrap();
    assert!(!manager.list().unwrap()[0].signature_verified);
    assert!(manager
        .install_signed(m.clone(), SOURCE, Some(signature(&m)))
        .is_err());
    assert!(
        manager
            .attest(&legacy.manifest_sha256, signature(&m))
            .unwrap()
            .signature_verified
    );
}
#[test]
fn packages_dependencies_missing_conflicting_immutable_and_uninstall_order() {
    let d = dir();
    let manager = Manager::open(&d.0.join("p"), false).unwrap();
    let a = manifest("helper", "v1");
    let b = manifest("helper", "v2");
    let parent = requires(manifest("parent", "v1"), vec![reference(&a)]);
    assert!(manager.install(parent.clone(), SOURCE).is_err());
    for m in [&a, &b, &parent] {
        manager.install(m.clone(), SOURCE).unwrap();
    }
    assert!(manager.uninstall(&a.identity().unwrap()).is_err());
    let conflict = requires(
        manifest("root", "v1"),
        vec![reference(&parent), reference(&b)],
    );
    assert!(manager.install(conflict, SOURCE).is_err());
    let mut unknown = requires(manifest("other", "v1"), vec![]);
    unknown
        .package
        .as_mut()
        .unwrap()
        .platform
        .push("download_new_library".into());
    assert!(manager.install(unknown, SOURCE).is_err());
    drop(manager);
    let manager = Manager::open(&d.0.join("p"), false).unwrap();
    manager.uninstall(&parent.identity().unwrap()).unwrap();
    manager.uninstall(&a.identity().unwrap()).unwrap();
    manager.uninstall(&b.identity().unwrap()).unwrap();
}
#[cfg(unix)]
#[test]
fn packages_trust_file_permissions_and_symlinks_rejected() {
    use std::os::unix::fs::PermissionsExt;
    let d = dir();
    let file = d.0.join("trust.json");
    std::fs::write(&file, serde_json::to_vec(&policy()).unwrap()).unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    TrustPolicy::from_file(&file).unwrap();
    let link = d.0.join("link");
    std::os::unix::fs::symlink(&file, &link).unwrap();
    assert!(TrustPolicy::from_file(&link).is_err());
    let fifo = d.0.join("fifo");
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    assert!(TrustPolicy::from_file(&fifo).is_err());
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o666)).unwrap();
    assert!(TrustPolicy::from_file(&file).is_err());
}

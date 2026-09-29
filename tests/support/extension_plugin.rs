//! Prebuilt standalone SDK fixture. Explicit ignored tests require these two
//! paths; ordinary tests never silently skip a missing executable or compile.
use super::plugin_runtime as p;
use std::path::PathBuf;
pub struct Dir(pub PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn dir() -> Dir {
    let path = std::env::temp_dir().join(format!(
        "sparrow-extension-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    Dir(path)
}
pub fn sample(role: &str) -> (p::Manifest, Vec<u8>) {
    let root = PathBuf::from(
        std::env::var_os("SPARROW_EXTENSION_EXAMPLES")
            .expect("build-extension-example.sh output directory required"),
    );
    (
        serde_json::from_slice(&std::fs::read(root.join(format!("{role}.json"))).unwrap()).unwrap(),
        std::fs::read(root.join("artifact.elf")).unwrap(),
    )
}
pub fn fault(role: &str) -> (p::Manifest, Vec<u8>) {
    let (mut manifest, _) = sample(role);
    let bytes = std::fs::read(
        std::env::var_os("SPARROW_EXTENSION_FAULT").expect("conformance fault executable required"),
    )
    .unwrap();
    manifest.name = format!("fault_{role}");
    manifest.artifact_sha256 = p::sha256(&bytes);
    let d = manifest
        .package
        .as_mut()
        .unwrap()
        .extension
        .as_mut()
        .unwrap();
    d.permissions.clear();
    d.watermarks = role == "source";
    for field in &mut d.output {
        field.nullable = true;
    }
    (manifest, bytes)
}
pub fn install(manager: &p::Manager, manifest: p::Manifest, bytes: &[u8]) -> p::extension::Binding {
    let info = manager.install(manifest, bytes).unwrap();
    manager
        .enable(&info.manifest_sha256, &info.manifest_sha256)
        .unwrap();
    p::extension::Binding {
        name: info.manifest.name,
        version: info.manifest.version,
        manifest_sha256: info.manifest_sha256,
        config: serde_json::Value::Null,
    }
}
pub fn manager(root: &std::path::Path) -> std::sync::Arc<p::Manager> {
    p::Manager::open_with_extensions(root, false, None, None, Default::default(), true).unwrap()
}

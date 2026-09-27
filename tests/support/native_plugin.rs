//! Shared Linux native fixture; build outputs live only in a private temp dir.
use std::{path::PathBuf, sync::OnceLock};
pub struct Dir(pub PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn dir() -> Dir {
    let p = std::env::temp_dir().join(format!(
        "sparrow-plugin-fixture-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&p).unwrap();
    Dir(p)
}
pub fn artifact() -> &'static (Vec<u8>, String) {
    static VALUE: OnceLock<(Vec<u8>, String)> = OnceLock::new();
    VALUE.get_or_init(|| {
        let d = dir();
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let output = d.0.join("sample.so");
        assert!(std::process::Command::new("cc")
            .args(["-std=c11", "-O2", "-fPIC", "-shared", "-Wall", "-Wextra", "-Werror"])
            .arg(format!("-I{}", root.join("sdk/native").display()))
            .arg(root.join("examples/plugins/native_math.c"))
            .arg("-o")
            .arg(&output)
            .status()
            .unwrap()
            .success());
        let hash = std::process::Command::new("sha256sum")
            .arg(&output)
            .output()
            .unwrap();
        assert!(hash.status.success());
        (
            std::fs::read(output).unwrap(),
            String::from_utf8(hash.stdout)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .into(),
        )
    })
}
pub fn manifest() -> serde_json::Value {
    serde_json::json!({"format":1,"name":"native_math","version":"v1","kind":"native_scalar","abi":1,"semantics":1,
    "target":if cfg!(target_arch="aarch64"){"aarch64-unknown-linux-gnu"}else{"x86_64-unknown-linux-gnu"},"artifact_sha256":artifact().1,"deterministic":true,"thread_safe":true,"null_policy":"propagate",
    "functions":[{"name":"double","id":1,"inputs":["int64"],"output":"int64","max_output_bytes":1}]})
}
pub fn sql(digest: &str) -> String {
    format!("SELECT plugin_call('native_math','v1','{digest}','double',value) AS doubled FROM s")
}

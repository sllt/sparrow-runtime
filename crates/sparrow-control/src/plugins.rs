//! Plugin management boundary; no SQL catalog schema migration is needed.
use base64::Engine;
use serde::Deserialize;
pub use sparrow_expr::plugins::{
    resident_count, sha256, Manager, Manifest, PackageInfo, MAX_ARTIFACT,
};
use sparrow_model::{ErrorCode, Result, SparrowError};
pub const MAX_INSTALL_BODY: usize = 6 * 1024 * 1024;
pub fn script_worker_slots() -> usize {
    sparrow_expr::plugins::script::worker_count()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Install {
    manifest: Manifest,
    artifact_base64: String,
    #[serde(default)]
    signature: Option<sparrow_expr::plugins::trust::Signature>,
}
pub fn configure_from_env(store: &crate::Store, safe_mode: bool) -> Result<()> {
    let native = match std::env::var("SPARROW_ENABLE_NATIVE_PLUGINS").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        _ => return Err(invalid("SPARROW_ENABLE_NATIVE_PLUGINS must be 0 or 1")),
    };
    let script = match std::env::var("SPARROW_ENABLE_SCRIPT_PLUGINS").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        _ => return Err(invalid("SPARROW_ENABLE_SCRIPT_PLUGINS must be 0 or 1")),
    };
    let worker = if script && !safe_mode {
        Some(match std::env::var_os("SPARROW_JS_WORKER") {
            Some(path) => path.into(),
            None => std::env::current_exe()
                .map_err(|_| invalid("cannot locate JavaScript worker"))?
                .with_file_name("sparrow-js-worker"),
        })
    } else {
        None
    };
    let wasm = match std::env::var("SPARROW_ENABLE_WASM_PLUGINS").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        _ => return Err(invalid("SPARROW_ENABLE_WASM_PLUGINS must be 0 or 1")),
    };
    let wasm_worker = if wasm && !safe_mode {
        Some(match std::env::var_os("SPARROW_WASM_WORKER") {
            Some(path) => path.into(),
            None => std::env::current_exe()
                .map_err(|_| invalid("cannot locate WASM worker"))?
                .with_file_name("sparrow-wasm-worker"),
        })
    } else {
        None
    };
    match std::env::var_os("SPARROW_PLUGIN_DIR") {
        Some(root) => store.configure_plugins(Manager::open_with_policy(
            std::path::Path::new(&root),
            native && !safe_mode,
            worker,
            wasm_worker,
            match std::env::var_os("SPARROW_PLUGIN_TRUST_STORE") {
                Some(path) => sparrow_expr::plugins::trust::TrustPolicy::from_file(
                    std::path::Path::new(&path),
                )?,
                None => Default::default(),
            },
        )?),
        None if native || script || wasm => {
            Err(invalid("plugins require an explicit SPARROW_PLUGIN_DIR"))
        }
        None => Ok(()),
    }
}
pub fn manager(store: &crate::Store) -> Result<std::sync::Arc<Manager>> {
    store.plugins().ok_or_else(|| {
        SparrowError::new(
            ErrorCode::FeatureUnavailable,
            "plugin management requires SPARROW_PLUGIN_DIR",
        )
    })
}
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}
pub fn install(store: &crate::Store, bytes: &[u8]) -> Result<PackageInfo> {
    if bytes.len() > MAX_INSTALL_BODY {
        return Err(invalid("plugin install body exceeds 6MiB"));
    }
    let body: Install =
        serde_json::from_slice(bytes).map_err(|_| invalid("invalid strict plugin install JSON"))?;
    body.manifest.validate()?;
    if body.artifact_base64.len() > MAX_ARTIFACT.div_ceil(3) * 4 {
        return Err(invalid("plugin artifact exceeds 4MiB"));
    }
    let artifact = base64::engine::general_purpose::STANDARD
        .decode(&body.artifact_base64)
        .map_err(|_| invalid("invalid plugin base64"))?;
    manager(store)?.install_signed(body.manifest, &artifact, body.signature)
}

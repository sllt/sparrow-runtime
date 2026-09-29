use crate::{
    invalid, native::Native, FunctionDef, Manifest, MAX_ARTIFACT, MAX_MANIFEST, MAX_PACKAGES,
};
use serde::Serialize;
use sparrow_model::{DataType, ErrorCode, Result, Scalar, SparrowError};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

struct Loaded {
    backend: Backend,
    pins: AtomicUsize,
    _lock: Arc<sparrow_io::fs_lock::FileLock>,
}
enum Backend {
    Native(Native),
    Script(crate::script::Script),
}
impl Backend {
    fn load(manifest: &Manifest, bytes: &[u8], worker: Option<&Path>) -> Result<Self> {
        if manifest.is_script() {
            Ok(Self::Script(crate::script::Script::load(
                manifest,
                bytes,
                worker.ok_or_else(|| invalid("JavaScript worker is not configured"))?,
            )?))
        } else {
            Ok(Self::Native(Native::load(manifest, bytes)?))
        }
    }
}
struct Entry {
    manifest: Manifest,
    digest: String,
    desired: bool,
    enabled: bool,
    resident_attempted: bool,
    loaded: Option<Arc<Loaded>>,
}
#[derive(Clone, Debug, Serialize)]
pub struct PackageInfo {
    pub manifest: Manifest,
    pub manifest_sha256: String,
    pub desired_enabled: bool,
    pub enabled: bool,
    pub resident: bool,
    pub pins: usize,
    pub hot_unload: bool,
    /// Not a liveness probe: a child exit is observed on the next call.
    pub script_worker_state: Option<&'static str>,
}
impl Entry {
    fn info(&self) -> PackageInfo {
        PackageInfo {
            manifest: self.manifest.clone(),
            manifest_sha256: self.digest.clone(),
            desired_enabled: self.desired,
            enabled: self.enabled,
            resident: self.resident_attempted || self.loaded.is_some(),
            pins: self
                .loaded
                .as_ref()
                .map_or(0, |l| l.pins.load(Ordering::SeqCst)),
            hot_unload: self.manifest.is_script(),
            script_worker_state: if self.manifest.is_script() {
                Some(match self.loaded.as_ref().map(|l| &l.backend) {
                    Some(Backend::Script(script)) => script.state(),
                    _ => "unloaded",
                })
            } else {
                None
            },
        }
    }
}
pub struct Manager {
    root: PathBuf,
    allow_native: bool,
    script_worker: Option<PathBuf>,
    entries: Mutex<BTreeMap<String, Entry>>,
    epoch: AtomicU64,
    _lock: Arc<sparrow_io::fs_lock::FileLock>,
}
impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginManager")
            .field("native_enabled", &self.allow_native)
            .finish_non_exhaustive()
    }
}
/// A live expression owns this pin; cloning an Arc cannot release it early.
pub struct Function {
    loaded: Arc<Loaded>,
    definition: FunctionDef,
    package: String,
    version: String,
    digest: String,
}
impl std::fmt::Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginFunction")
            .field("package", &self.package)
            .field("version", &self.version)
            .field("digest", &self.digest)
            .field("name", &self.definition.name)
            .finish()
    }
}
impl PartialEq for Function {
    fn eq(&self, o: &Self) -> bool {
        self.digest == o.digest && self.definition == o.definition
    }
}
impl Drop for Function {
    fn drop(&mut self) {
        self.loaded.pins.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Function {
    pub fn is_script(&self) -> bool {
        matches!(&self.loaded.backend, Backend::Script(_))
    }
    pub fn definition(&self) -> &FunctionDef {
        &self.definition
    }
    pub fn package(&self) -> &str {
        &self.package
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
    pub fn signature(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() != self.definition.inputs.len()
            || types
                .iter()
                .zip(&self.definition.inputs)
                .any(|(t, e)| *t != DataType::Null && *t != e.data_type())
        {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "plugin function argument signature mismatch",
            ));
        }
        Ok(self.definition.output.data_type())
    }
    pub fn invoke(&self, args: &[Scalar]) -> Result<Scalar> {
        let result = match &self.loaded.backend {
            Backend::Native(n) => n.invoke(&self.definition, args),
            Backend::Script(s) => s.invoke(&self.definition, args),
        };
        result.map_err(|e| {
            e.context("plugin_package", &self.package)
                .context("plugin_version", &self.version)
                .context("plugin_function", &self.definition.name)
        })
    }
    pub fn scratch_bytes(&self) -> usize {
        if self.is_script() {
            return crate::script::scratch_bytes(&self.definition);
        }
        self.definition
            .max_output_bytes
            .saturating_mul(3)
            .saturating_add(1024)
    }
}
fn io(_: std::io::Error) -> SparrowError {
    SparrowError::new(
        ErrorCode::Internal,
        "plugin package storage operation failed",
    )
}
fn read(path: &Path, cap: usize) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path).map_err(io)?;
    let meta = file.metadata().map_err(io)?;
    if !meta.is_file() || meta.len() > cap as u64 {
        return Err(invalid("plugin file type/size rejected"));
    }
    let mut data = Vec::new();
    file.take(cap as u64 + 1)
        .read_to_end(&mut data)
        .map_err(io)?;
    if data.len() > cap {
        return Err(invalid("plugin file grew beyond limit"));
    }
    Ok(data)
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io)?;
    file.write_all(bytes).map_err(io)?;
    file.sync_all().map_err(io)
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path).map_err(io)?.sync_all().map_err(io)
}
struct Staging(PathBuf);
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
impl Manager {
    pub fn open(root: &Path, allow_native: bool) -> Result<Arc<Self>> {
        Self::open_with_scripts(root, allow_native, None)
    }
    pub fn open_with_scripts(
        root: &Path,
        allow_native: bool,
        script_worker: Option<PathBuf>,
    ) -> Result<Arc<Self>> {
        if let Some(worker) = &script_worker {
            crate::script::validate_worker(worker)?;
        }
        // Parent directory is administrator-owned. Refuse a symlink or writable
        // package root; artifact loads additionally use a verified sealed copy.
        if !root.exists() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = std::fs::DirBuilder::new();
                builder.mode(0o700).create(root).map_err(io)?;
            }
            #[cfg(not(unix))]
            {
                std::fs::create_dir(root).map_err(io)?;
            }
        }
        let meta = std::fs::symlink_metadata(root).map_err(io)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(invalid("plugin root must be a real directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.mode() & 0o022 != 0 || meta.uid() != unsafe { libc::geteuid() } {
                return Err(invalid(
                    "plugin root must be owned by service user and not group/world writable",
                ));
            }
        }
        let lock = Arc::new(sparrow_io::fs_lock::FileLock::acquire(&root.join(".lock"))?);
        let mut entries = BTreeMap::new();
        let mut directory_count = 0usize;
        for item in std::fs::read_dir(root).map_err(io)? {
            directory_count += 1;
            if directory_count > 64 {
                return Err(invalid("plugin root exceeds bounded directory inventory"));
            }
            let item = item.map_err(io)?;
            let name = item.file_name();
            let Some(name) = name.to_str() else {
                return Err(invalid("invalid plugin directory name"));
            };
            if name == ".lock" {
                continue;
            }
            // Incomplete, never published installation from an interrupted process.
            if name.starts_with(".staging-") {
                if !item.file_type().map_err(io)?.is_dir() {
                    return Err(invalid("invalid plugin staging directory"));
                }
                std::fs::remove_dir_all(item.path()).map_err(io)?;
                continue;
            }
            if entries.len() >= MAX_PACKAGES
                || !crate::digest_name(name)
                || !item.file_type().map_err(io)?.is_dir()
            {
                return Err(invalid("invalid/excessive installed plugin directory"));
            }
            let manifest: Manifest =
                serde_json::from_slice(&read(&item.path().join("manifest.json"), MAX_MANIFEST)?)
                    .map_err(|_| invalid("invalid installed manifest"))?;
            let digest = manifest.identity()?;
            if digest != name
                || entries.values().any(|e: &Entry| {
                    e.manifest.name == manifest.name && e.manifest.version == manifest.version
                })
            {
                return Err(invalid("installed plugin identity/version conflict"));
            }
            let bytes = read(&item.path().join(manifest.artifact_name()), MAX_ARTIFACT)?;
            manifest.check_artifact(&bytes)?;
            let marker = item.path().join("enabled");
            let desired = if marker.exists() {
                if read(&marker, 64)? != digest.as_bytes() {
                    return Err(invalid("plugin approval marker mismatch"));
                }
                true
            } else {
                false
            };
            let allowed = if manifest.is_script() {
                script_worker.is_some()
            } else {
                allow_native
            };
            let loaded = if desired && allowed {
                Some(Arc::new(Loaded {
                    backend: Backend::load(&manifest, &bytes, script_worker.as_deref())?,
                    pins: AtomicUsize::new(0),
                    _lock: lock.clone(),
                }))
            } else {
                None
            };
            entries.insert(
                digest.clone(),
                Entry {
                    resident_attempted: loaded.is_some() && !manifest.is_script(),
                    manifest,
                    digest,
                    desired,
                    enabled: desired && allowed,
                    loaded,
                },
            );
        }
        Ok(Arc::new(Self {
            root: root.to_owned(),
            allow_native,
            script_worker,
            entries: Mutex::new(entries),
            epoch: AtomicU64::new(0),
            _lock: lock,
        }))
    }
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
    pub fn native_allowed(&self) -> bool {
        self.allow_native
    }
    pub fn script_allowed(&self) -> bool {
        self.script_worker.is_some()
    }
    fn entries(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, Entry>>> {
        self.entries
            .lock()
            .map_err(|_| SparrowError::new(ErrorCode::Internal, "plugin registry lock poisoned"))
    }
    pub fn list(&self) -> Result<Vec<PackageInfo>> {
        Ok(self.entries()?.values().map(Entry::info).collect())
    }
    pub fn install(&self, manifest: Manifest, bytes: &[u8]) -> Result<PackageInfo> {
        manifest.check_artifact(bytes)?;
        let encoded = manifest.bytes()?;
        let digest = crate::sha256(&encoded);
        let mut entries = self.entries()?;
        if let Some(existing) = entries.get(&digest) {
            return Ok(existing.info());
        }
        if entries.len() >= MAX_PACKAGES
            || entries
                .values()
                .any(|e| e.manifest.name == manifest.name && e.manifest.version == manifest.version)
        {
            return Err(invalid(
                "plugin version is immutable or package count exceeded",
            ));
        }
        let staging = Staging(self.root.join(format!(
            ".staging-{}-{}",
            std::process::id(),
            self.epoch()
        )));
        std::fs::create_dir(&staging.0).map_err(io)?;
        write_new(&staging.0.join("manifest.json"), &encoded)?;
        write_new(&staging.0.join(manifest.artifact_name()), bytes)?;
        sync_dir(&staging.0)?;
        std::fs::rename(&staging.0, self.root.join(&digest)).map_err(io)?;
        sync_dir(&self.root)?;
        let entry = Entry {
            manifest,
            digest: digest.clone(),
            desired: false,
            enabled: false,
            resident_attempted: false,
            loaded: None,
        };
        let info = entry.info();
        entries.insert(digest, entry);
        self.epoch.fetch_add(1, Ordering::SeqCst);
        Ok(info)
    }
    pub fn enable(&self, digest: &str, approve: &str) -> Result<PackageInfo> {
        if !crate::digest_name(digest) || approve != digest {
            return Err(invalid(
                "explicit approval must equal exact manifest SHA256",
            ));
        }
        let mut entries = self.entries()?;
        let entry = entries
            .get_mut(digest)
            .ok_or_else(|| invalid("plugin not installed"))?;
        if if entry.manifest.is_script() {
            !self.script_allowed()
        } else {
            !self.allow_native
        } {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "plugin backend is disabled by server configuration",
            ));
        }
        if entry.enabled {
            if matches!(entry.loaded.as_ref().map(|l| &l.backend), Some(Backend::Script(s)) if s.state()=="failed")
            {
                return Err(invalid(
                    "JavaScript worker failed; stop users, disable and re-enable package",
                ));
            }
            return Ok(entry.info());
        }
        if entry.loaded.is_none() {
            let bytes = read(
                &self.root.join(digest).join(entry.manifest.artifact_name()),
                MAX_ARTIFACT,
            )?;
            if entry.resident_attempted {
                return Err(invalid(
                    "previous native activation failed; restart before retry",
                ));
            }
            entry.resident_attempted = !entry.manifest.is_script();
            entry.loaded = Some(Arc::new(Loaded {
                backend: Backend::load(&entry.manifest, &bytes, self.script_worker.as_deref())?,
                pins: AtomicUsize::new(0),
                _lock: self._lock.clone(),
            }));
        }
        let dir = self.root.join(digest);
        let marker = dir.join("enabled");
        if !marker.exists() {
            write_new(&marker, digest.as_bytes())?;
            sync_dir(&dir)?;
        } else if read(&marker, 64)? != digest.as_bytes() {
            return Err(invalid("plugin approval marker mismatch"));
        }
        entry.desired = true;
        entry.enabled = true;
        self.epoch.fetch_add(1, Ordering::SeqCst);
        Ok(entry.info())
    }
    pub fn disable(&self, digest: &str) -> Result<PackageInfo> {
        let mut entries = self.entries()?;
        let entry = entries
            .get_mut(digest)
            .ok_or_else(|| invalid("plugin not installed"))?;
        if entry.info().pins > 0 {
            return Err(SparrowError::new(
                ErrorCode::ResourceExhausted,
                "plugin is pinned by a plan/job; stop users before disabling",
            ));
        }
        let marker = self.root.join(digest).join("enabled");
        if marker.exists() {
            std::fs::remove_file(marker).map_err(io)?;
            sync_dir(&self.root.join(digest))?;
        }
        entry.enabled = false;
        entry.desired = false;
        if entry.manifest.is_script() {
            entry.loaded = None;
        }
        self.epoch.fetch_add(1, Ordering::SeqCst);
        Ok(entry.info())
    }
    pub fn uninstall(&self, digest: &str) -> Result<()> {
        let mut entries = self.entries()?;
        let entry = entries
            .get(digest)
            .ok_or_else(|| invalid("plugin not installed"))?;
        if entry.desired || entry.enabled || entry.resident_attempted {
            return Err(invalid(
                "disable then restart before uninstalling any resident native package",
            ));
        }
        // Rename out of the published namespace before recursive removal.
        let trash = Staging(self.root.join(format!(
            ".staging-remove-{}-{}",
            std::process::id(),
            self.epoch()
        )));
        std::fs::rename(self.root.join(digest), &trash.0).map_err(io)?;
        sync_dir(&self.root)?;
        entries.remove(digest);
        self.epoch.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    pub fn resolve(
        &self,
        name: &str,
        version: &str,
        digest: &str,
        function: &str,
    ) -> Result<Arc<Function>> {
        let entries = self.entries()?;
        let entry = entries
            .get(digest)
            .filter(|e| e.enabled && e.manifest.name == name && e.manifest.version == version)
            .ok_or_else(|| {
                invalid("plugin missing, disabled or dependency hash/version mismatch")
            })?;
        let definition = entry
            .manifest
            .functions
            .iter()
            .find(|f| f.name == function)
            .ok_or_else(|| invalid("plugin function not declared"))?
            .clone();
        let loaded = entry
            .loaded
            .as_ref()
            .ok_or_else(|| invalid("plugin backend not loaded"))?
            .clone();
        if matches!(&loaded.backend, Backend::Script(s) if s.state()=="failed") {
            return Err(invalid(
                "JavaScript worker failed; stop users, disable and re-enable package",
            ));
        }
        loaded.pins.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Function {
            loaded,
            definition,
            package: name.into(),
            version: version.into(),
            digest: digest.into(),
        }))
    }
}

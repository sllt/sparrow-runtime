//! Bounded append-only NDJSON segments. Linux directory-FD anchoring, a
//! cooperative writer lock, no overwrite, no automatic deletion or replay.
use crate::{ConnectorError, IoDiagnostics, Result};
use sparrow_formats::action::ActionSpec;
use sparrow_io::{fs_lock::FileLock, observed::Receiver};
use sparrow_model::observation::HealthState;
use sparrow_model::{CreditKind, ErrorCode, InflightCounter, RowBatch};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
};
use tokio_util::sync::CancellationToken;

const MARKER: &[u8] = b"SPARROW_NDJSON_SINK_V1\n";
const LOCK: &str = "WRITER_LOCK";
const FORMAT: &str = "FORMAT";

#[derive(Clone, Debug)]
pub struct FileSinkConfig {
    pub directory: PathBuf,
    pub segment_bytes: u64,
    pub max_bytes: u64,
    pub max_files: usize,
    pub row_bytes: usize,
    pub sync_data: bool,
}
impl FileSinkConfig {
    pub fn validate(&self) -> Result<()> {
        if !cfg!(target_os = "linux") {
            return Err(ConnectorError::new(
                ErrorCode::FeatureUnavailable,
                "File Sink v1 requires Linux directory-FD anchoring",
            ));
        }
        if !(1024..=64 * 1024 * 1024).contains(&self.segment_bytes)
            || self.max_bytes < self.segment_bytes
            || self.max_bytes > 1024 * 1024 * 1024 * 1024
            || !(1..=1024).contains(&self.max_files)
            || !(2..=1024 * 1024).contains(&self.row_bytes)
            || self.row_bytes as u64 + 1 > self.segment_bytes
        {
            return Err(ConnectorError::new(ErrorCode::BoundExceeded,"File Sink requires segment_bytes=1KiB..64MiB, max_bytes=segment..1TiB, max_files=1..1024, row_bytes=2..1MiB fitting one segment including newline"));
        }
        crate::policy::check_data_path(&self.directory)?;
        let meta = std::fs::symlink_metadata(&self.directory).map_err(io)?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(denied(
                "File Sink requires an existing, dedicated non-symlink directory",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o022 != 0 {
                return Err(denied(
                    "File Sink directory must not be group/world writable",
                ));
            }
        }
        Ok(())
    }
}
fn io(e: std::io::Error) -> ConnectorError {
    ConnectorError::new(ErrorCode::Internal, format!("File Sink I/O: {e}"))
}
fn denied(message: &str) -> ConnectorError {
    ConnectorError::new(ErrorCode::PolicyDenied, message)
}
fn exhausted(message: &str) -> ConnectorError {
    ConnectorError::new(ErrorCode::ResourceExhausted, message)
}
fn model(e: sparrow_model::SparrowError) -> ConnectorError {
    ConnectorError::new(e.code, e.message)
}
fn regular(path: &Path) -> Result<std::fs::Metadata> {
    let meta = std::fs::symlink_metadata(path).map_err(io)?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(denied("File Sink entries must be regular files"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() != 1 {
            return Err(denied("File Sink refuses hard-linked entries"));
        }
    }
    Ok(meta)
}
fn create(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(io)
}
struct Writer {
    file: Option<File>,
    _lock: FileLock,
    directory: File,
    anchored: PathBuf,
    config: FileSinkConfig,
    bytes: u64,
    current_bytes: u64,
    files: usize,
    next: u64,
}
impl Writer {
    fn open(config: FileSinkConfig) -> Result<Self> {
        config.validate()?;
        let directory = File::open(&config.directory).map_err(io)?;
        let opened = directory.metadata().map_err(io)?;
        let named = std::fs::symlink_metadata(&config.directory).map_err(io)?;
        if !opened.is_dir() || !named.is_dir() || named.file_type().is_symlink() {
            return Err(denied("File Sink directory changed"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if opened.dev() != named.dev() || opened.ino() != named.ino() {
                return Err(denied("File Sink directory identity changed"));
            }
        }
        // Recheck the original policy before creating anything. All subsequent
        // operations address THIS held descriptor, never a re-resolved parent.
        crate::policy::check_data_path(&config.directory)?;
        #[cfg(target_os = "linux")]
        let anchored = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()))
        };
        #[cfg(not(target_os = "linux"))]
        let anchored = config.directory.clone(); // validate rejects this platform
        Self::scan(&anchored, &config, false)?; // do not add a lock to a foreign directory
        let lock = FileLock::acquire(&anchored.join(LOCK)).map_err(model)?;
        let (bytes, files, next, marked) = Self::scan(&anchored, &config, true)?;
        if !marked {
            let mut marker = create(&anchored.join(FORMAT))?;
            marker.write_all(MARKER).map_err(io)?;
            marker.sync_all().map_err(io)?;
            directory.sync_all().map_err(io)?;
        }
        Ok(Self {
            file: None,
            _lock: lock,
            directory,
            anchored,
            config,
            bytes,
            current_bytes: 0,
            files,
            next,
        })
    }
    fn scan(
        dir: &Path,
        config: &FileSinkConfig,
        check_tail: bool,
    ) -> Result<(u64, usize, u64, bool)> {
        let (mut bytes, mut files, mut next, mut marked) = (0u64, 0usize, 1u64, false);
        let mut entries = 0;
        for entry in std::fs::read_dir(dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            entries += 1;
            if entries > config.max_files + 2 {
                return Err(exhausted("File Sink directory exceeds file count"));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| denied("non-UTF8 file in File Sink directory"))?;
            let path = dir.join(name);
            let meta = regular(&path)?;
            if name == LOCK {
                continue;
            }
            if name == FORMAT {
                if meta.len() != MARKER.len() as u64 {
                    return Err(denied("invalid File Sink FORMAT size"));
                }
                let raw = std::fs::read(&path).map_err(io)?;
                if raw != MARKER {
                    return Err(denied("foreign or incomplete File Sink FORMAT"));
                }
                marked = true;
                continue;
            }
            let text = name
                .strip_prefix("part-")
                .and_then(|s| s.strip_suffix(".ndjson"))
                .filter(|s| s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()))
                .ok_or_else(|| denied("foreign file in File Sink directory"))?;
            let id = text
                .parse::<u64>()
                .map_err(|_| denied("invalid File Sink segment number"))?;
            if id == 0 {
                return Err(denied("invalid File Sink segment zero"));
            }
            next = next.max(
                id.checked_add(1)
                    .ok_or_else(|| exhausted("File Sink segment sequence exhausted"))?,
            );
            files += 1;
            bytes = bytes
                .checked_add(meta.len())
                .ok_or_else(|| exhausted("File Sink byte count overflow"))?;
            if meta.len() > config.segment_bytes
                || bytes > config.max_bytes
                || files > config.max_files
            {
                return Err(exhausted(
                    "existing File Sink data exceeds configured bounds",
                ));
            }
            if check_tail && meta.len() > 0 {
                let mut file = File::open(path).map_err(io)?;
                file.seek(SeekFrom::End(-1)).map_err(io)?;
                let mut last = [0];
                file.read_exact(&mut last).map_err(io)?;
                if last != [b'\n'] {
                    return Err(denied(
                        "incomplete File Sink tail; preserve and repair offline before restart",
                    ));
                }
            }
        }
        if files > 0 && !marked {
            return Err(denied("File Sink data without FORMAT; refusing adoption"));
        }
        Ok((bytes, files, next, marked))
    }
    fn row(&mut self, bytes: &[u8], diag: &IoDiagnostics) -> Result<()> {
        let size = (bytes.len() as u64)
            .checked_add(1)
            .ok_or_else(|| exhausted("File Sink row overflow"))?;
        if size > self.config.segment_bytes
            || self
                .bytes
                .checked_add(size)
                .is_none_or(|n| n > self.config.max_bytes)
        {
            return Err(exhausted("File Sink capacity reached; no data was deleted"));
        }
        if self.file.is_none() || self.current_bytes + size > self.config.segment_bytes {
            if self.files >= self.config.max_files {
                return Err(exhausted(
                    "File Sink file limit reached; no data was deleted",
                ));
            }
            self.flush(diag)?;
            let next = self
                .next
                .checked_add(1)
                .ok_or_else(|| exhausted("File Sink segment sequence exhausted"))?;
            let file = create(&self.anchored.join(format!("part-{:020}.ndjson", self.next)))?;
            if self.config.sync_data {
                self.directory.sync_all().map_err(io)?;
            }
            self.file = Some(file);
            self.next = next;
            self.files += 1;
            self.current_bytes = 0;
            diag.file_segments.fetch_add(1, Ordering::Relaxed);
        }
        let file = self.file.as_mut().expect("created segment");
        // A failed/partial write is never retried in place or counted as an
        // accepted row. Its evidence remains for explicit offline inspection.
        file.write_all(bytes).map_err(io)?;
        file.write_all(b"\n").map_err(io)?;
        self.current_bytes += size;
        self.bytes += size;
        diag.file_written.fetch_add(1, Ordering::Relaxed);
        diag.file_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(())
    }
    fn flush(&mut self, diag: &IoDiagnostics) -> Result<()> {
        if let Some(file) = &mut self.file {
            file.flush().map_err(io)?;
            if self.config.sync_data {
                file.sync_data().map_err(io)?;
                diag.file_syncs.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
    fn batch(
        &mut self,
        batch: &RowBatch,
        action: &ActionSpec,
        diag: &IoDiagnostics,
        cancel: &CancellationToken,
    ) -> Result<()> {
        if batch.output_sequence().is_some() {
            return Err(ConnectorError::new(
                ErrorCode::UnsupportedRestore,
                "File Sink has no reliable output receipt",
            ));
        }
        let owner = batch.lease().owner();
        for row in batch.rows() {
            if cancel.is_cancelled() {
                return Err(ConnectorError::new(
                    ErrorCode::JobFailed,
                    "File Sink cancelled before batch completion",
                ));
            }
            let mut credit = owner
                .acquire(CreditKind::Reservation, 8192)
                .map_err(model)?;
            let bytes = action
                .encode(
                    batch.schema(),
                    std::slice::from_ref(row),
                    false,
                    self.config.row_bytes,
                    |cap| credit.grow_to(cap + 8192),
                )
                .map_err(model)?;
            self.row(&bytes, diag)?; // bytes drops before its credit
        }
        self.flush(diag)
    }
}

pub struct FileSink {
    pub config: FileSinkConfig,
    pub diag: Arc<IoDiagnostics>,
    pub action: ActionSpec,
}
impl FileSink {
    pub async fn run(
        self,
        mut rx: Receiver<RowBatch>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let _lifecycle = self.diag.observation.lifecycle(false);
        let config = self.config;
        let diag = self.diag.clone();
        let opened = tokio::task::spawn_blocking(move || Writer::open(config)).await;
        let mut writer = match opened {
            Ok(Ok(writer)) => Some(writer),
            result => {
                let error = match result {
                    Ok(Err(e)) => e,
                    Err(e) => ConnectorError::new(ErrorCode::Internal, e.to_string()),
                    _ => unreachable!(),
                };
                record_failure(&diag, &error);
                None
            }
        };
        if writer.is_some() {
            self.diag.observation.health(
                false,
                HealthState::Ready,
                "file_sink_open_not_replayable",
                None,
            );
        }
        let action = Arc::new(self.action);
        while writer.is_some() {
            let batch = tokio::select! {biased;_=cancel.cancelled()=>break,batch=rx.recv()=>batch};
            let Some(batch) = batch else { break };
            let mut delivery = diag.observation.delivery_guard(
                batch.num_rows(),
                batch.tracked_bytes(),
                batch.origin(),
            );
            let rows = batch.num_rows();
            let mut owned = writer.take().expect("writer");
            let d = diag.clone();
            let a = action.clone();
            let c = cancel.clone();
            // Join even on cancellation: never release/reacquire the writer
            // lock while an OS write from the previous attempt still runs.
            let result = tokio::task::spawn_blocking(move || {
                let result = owned.batch(&batch, &a, &d, &c);
                (owned, result)
            })
            .await;
            match result {
                Ok((owned, Ok(()))) => {
                    writer = Some(owned);
                    delivery.complete();
                    diag.observation.progress(false, rows);
                    if let Some(o) = &outbox {
                        o.ack();
                    }
                }
                result => {
                    let error = match result {
                        Ok((_, Err(e))) => e,
                        Err(e) => ConnectorError::new(ErrorCode::Internal, e.to_string()),
                        _ => unreachable!(),
                    };
                    if !cancel.is_cancelled() {
                        record_failure(&diag, &error);
                    }
                    if let Some(o) = &outbox {
                        o.fail();
                    }
                    break;
                }
            }
        }
        rx.close();
        while let Ok(_batch) = rx.discard_next() {
            if let Some(o) = &outbox {
                o.fail();
            }
        }
        if diag.file_failed.load(Ordering::Relaxed) > 0 && outbox.is_none() {
            cancel.cancel();
        }
        // Each accepted batch already flushed. No detached write/flush worker.
        drop(writer);
    }
}
fn record_failure(diag: &IoDiagnostics, error: &ConnectorError) {
    diag.file_failed.fetch_add(1, Ordering::Relaxed);
    diag.observation.health(
        false,
        HealthState::Failed,
        "file_sink_failed",
        Some(error.code),
    );
    eprintln!("sparrow-file-sink: {error}");
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use sparrow_model::{
        DataType, Field, FieldId, MemoryOwner, ResourceBudget, Row, RowBatchBuilder, Scalar,
        Schema, SchemaId,
    };
    struct Dir(PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn directory() -> Dir {
        let dir = crate::ensure_default_data_root().join(format!(
            "actions-file-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        Dir(dir)
    }
    fn config(dir: &Dir) -> FileSinkConfig {
        FileSinkConfig {
            directory: dir.0.clone(),
            segment_bytes: 1024,
            max_bytes: 4096,
            max_files: 4,
            row_bytes: 1000,
            sync_data: true,
        }
    }
    fn batch(owner: &Arc<MemoryOwner>, text: &str) -> RowBatch {
        let schema = Arc::new(
            Schema::new(
                SchemaId::new(1),
                vec![Field::new(FieldId::new(1), "value", DataType::Utf8, false)],
            )
            .unwrap(),
        );
        let mut builder =
            RowBatchBuilder::new(schema, owner.clone(), CreditKind::Reservation, 1, 1024).unwrap();
        builder
            .push(Row {
                values: vec![Scalar::utf8(text)],
            })
            .unwrap();
        builder.finish().unwrap()
    }
    #[test]
    fn actions_file_rotation_restart_no_overwrite_and_lock() {
        let dir = directory();
        let cfg = config(&dir);
        let diag = IoDiagnostics::new();
        let mut writer = Writer::open(cfg.clone()).unwrap();
        assert!(Writer::open(cfg.clone()).is_err());
        writer.row(&vec![b'a'; 700], &diag).unwrap();
        writer.row(&vec![b'b'; 700], &diag).unwrap();
        writer.flush(&diag).unwrap();
        let first = dir.0.join("part-00000000000000000001.ndjson");
        let original = std::fs::read(&first).unwrap();
        drop(writer);
        let mut second = Writer::open(cfg).unwrap();
        second.row(b"{}", &diag).unwrap();
        second.flush(&diag).unwrap();
        drop(second);
        assert_eq!(std::fs::read(first).unwrap(), original);
        assert_eq!(
            std::fs::read(dir.0.join("part-00000000000000000003.ndjson")).unwrap(),
            b"{}\n"
        );
        assert_eq!(
            (diag.snapshot().file_written, diag.snapshot().file_segments),
            (3, 3)
        );
        assert!(diag.snapshot().file_syncs >= 3);
    }
    #[test]
    fn actions_file_quota_never_deletes_or_partially_admits_a_row() {
        let dir = directory();
        let mut cfg = config(&dir);
        cfg.max_bytes = 1024;
        cfg.max_files = 1;
        let mut writer = Writer::open(cfg.clone()).unwrap();
        let diag = IoDiagnostics::new();
        writer.row(&vec![b'a'; 700], &diag).unwrap();
        assert!(writer.row(&vec![b'b'; 700], &diag).is_err());
        assert_eq!(writer.bytes, 701);
        assert_eq!(diag.snapshot().file_written, 1);
        drop(writer);
        let before = std::fs::read(dir.0.join("part-00000000000000000001.ndjson")).unwrap();
        assert!(Writer::open(cfg).unwrap().row(b"{}", &diag).is_err());
        assert_eq!(
            before,
            std::fs::read(dir.0.join("part-00000000000000000001.ndjson")).unwrap()
        );
    }
    #[test]
    fn actions_file_refuses_foreign_linked_and_incomplete_data() {
        for variant in 0..5 {
            let dir = directory();
            let cfg = config(&dir);
            match variant {
                0 => std::fs::write(dir.0.join("foreign.txt"), b"original").unwrap(),
                1 => {
                    std::fs::write(dir.0.join(FORMAT), MARKER).unwrap();
                    std::fs::write(dir.0.join("part-00000000000000000001.ndjson"), b"{broken")
                        .unwrap();
                }
                2 => {
                    std::os::unix::fs::symlink("missing", dir.0.join(FORMAT)).unwrap();
                }
                3 => {
                    std::fs::write(dir.0.join(FORMAT), MARKER).unwrap();
                    std::fs::hard_link(
                        dir.0.join(FORMAT),
                        dir.0.join("part-00000000000000000001.ndjson"),
                    )
                    .unwrap();
                }
                _ => {
                    std::fs::write(dir.0.join("part-00000000000000000001.ndjson"), b"{}\n").unwrap()
                }
            }
            assert!(Writer::open(cfg).is_err(), "variant {variant}");
            if variant == 0 {
                assert!(!dir.0.join(LOCK).exists());
                assert_eq!(
                    std::fs::read(dir.0.join("foreign.txt")).unwrap(),
                    b"original"
                );
            }
        }
    }
    #[test]
    fn actions_file_directory_fd_survives_name_replacement() {
        let dir = directory();
        let cfg = config(&dir);
        let mut writer = Writer::open(cfg).unwrap();
        let moved = dir.0.with_extension("held");
        std::fs::rename(&dir.0, &moved).unwrap();
        std::fs::create_dir(&dir.0).unwrap();
        writer.row(b"{}", &IoDiagnostics::new()).unwrap();
        drop(writer);
        assert!(!dir.0.join("part-00000000000000000001.ndjson").exists());
        assert_eq!(
            std::fs::read(moved.join("part-00000000000000000001.ndjson")).unwrap(),
            b"{}\n"
        );
        std::fs::remove_dir_all(moved).unwrap();
    }
    #[tokio::test]
    async fn actions_file_async_receipt_mapping_and_budget_refund() {
        let dir = directory();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let diag = IoDiagnostics::new();
        let counter = Arc::new(InflightCounter::new());
        counter.enqueue();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(batch(&owner, "测\n")).await.unwrap();
        drop(tx);
        let action = ActionSpec {
            body: Some(serde_json::json!({"mapped":{"$field":"value"}})),
            ..Default::default()
        };
        FileSink {
            config: config(&dir),
            diag: diag.clone(),
            action,
        }
        .run(rx.into(), CancellationToken::new(), Some(counter.clone()))
        .await;
        assert_eq!(
            (counter.acked(), counter.failed(), counter.pending()),
            (1, 0, 0)
        );
        assert_eq!(
            std::fs::read_to_string(dir.0.join("part-00000000000000000001.ndjson")).unwrap(),
            "{\"mapped\":\"测\\n\"}\n"
        );
        assert_eq!(owner.usage().physical_bytes, 0);
        // Hard allocation failure leaves no data segment and fails, not ACKs.
        let mut budget = ResourceBudget::compact();
        budget.reservation_bytes = 9000;
        let limited = MemoryOwner::new(budget);
        let dir2 = directory();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let payload = batch(&limited, "x");
        let held = limited.acquire(CreditKind::Reservation, 1000).unwrap();
        tx.send(payload).await.unwrap();
        drop(tx);
        counter.enqueue();
        FileSink {
            config: config(&dir2),
            diag: diag.clone(),
            action: ActionSpec::default(),
        }
        .run(rx.into(), CancellationToken::new(), Some(counter.clone()))
        .await;
        assert_eq!(counter.failed(), 1);
        assert_eq!(diag.snapshot().file_failed, 1);
        drop(held);
        assert_eq!(limited.usage().physical_bytes, 0);
        assert!(!dir2.0.join("part-00000000000000000001.ndjson").exists());
    }
    #[tokio::test]
    async fn actions_file_cancel_drains_and_releases_writer() {
        let dir = directory();
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let counter = Arc::new(InflightCounter::new());
        counter.enqueue();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(batch(&owner, "x")).await.unwrap();
        drop(tx);
        let cancel = CancellationToken::new();
        cancel.cancel();
        FileSink {
            config: config(&dir),
            diag: IoDiagnostics::new(),
            action: ActionSpec::default(),
        }
        .run(rx.into(), cancel, Some(counter.clone()))
        .await;
        assert_eq!((counter.failed(), counter.pending()), (1, 0));
        assert_eq!(owner.usage().physical_bytes, 0);
        drop(Writer::open(config(&dir)).unwrap());
    }
}

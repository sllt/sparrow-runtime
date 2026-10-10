//! Separate bounded input quarantine. Purging closes the replay range through
//! the approved record; the persisted floor prevents recreating purged history.
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sparrow_io::{poison::InputQuarantine, SourcePosition};
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputDlqSpec {
    pub directory: String,
    pub max_disk_bytes: u64,
    pub max_payload_bytes: u64,
    pub max_entries: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PurgeRequest {
    pub approve_uuid: String,
    pub position: u64,
    pub approve_replay_floor: u64,
    pub approve_checkpoint: u64,
    pub reason: String,
}
pub(crate) fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}
fn db(e: rusqlite::Error) -> SparrowError {
    SparrowError::new(ErrorCode::Internal, format!("input DLQ database: {e}"))
}
fn io(e: std::io::Error) -> SparrowError {
    SparrowError::new(ErrorCode::Internal, format!("input DLQ filesystem: {e}"))
}
pub(crate) fn hash(b: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, b)
        .as_ref()
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect()
}
pub(crate) fn random_id() -> Result<String> {
    use ring::rand::SecureRandom;
    let mut b = [0u8; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut b)
        .map_err(|_| invalid("identity entropy unavailable"))?;
    Ok(b.iter().map(|v| format!("{v:02x}")).collect())
}
impl InputDlqSpec {
    pub fn validate(&self) -> Result<()> {
        if !Path::new(&self.directory).is_absolute()
            || self.directory.len() > 4096
            || !(8 * 1024 * 1024..=4 * 1024 * 1024 * 1024u64).contains(&self.max_disk_bytes)
            || !(65536..=self.max_disk_bytes / 4).contains(&self.max_payload_bytes)
            || !(1..=10000).contains(&self.max_entries)
        {
            return Err(invalid("input DLQ requires absolute directory, disk 8MiB..4GiB, payload 64KiB..disk/4 and entries 1..10000"));
        }
        sparrow_connectors::check_data_path(Path::new(&self.directory)).map_err(SparrowError::from)
    }
}
pub struct InputDlq {
    conn: Mutex<Connection>,
    spec: InputDlqSpec,
    binding: String,
    uuid: String,
    _lock: sparrow_io::fs_lock::FileLock,
}
static OPEN: OnceLock<Mutex<HashMap<PathBuf, Weak<InputDlq>>>> = OnceLock::new();
impl InputDlq {
    pub fn open(spec: &InputDlqSpec, binding: &str, create: bool) -> Result<Arc<Self>> {
        spec.validate()?;
        if create {
            std::fs::create_dir_all(&spec.directory).map_err(io)?;
        }
        let dir = std::fs::canonicalize(&spec.directory).map_err(io)?;
        sparrow_connectors::check_data_path(&dir).map_err(SparrowError::from)?;
        let mut open = OPEN
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| invalid("input DLQ registry poisoned"))?;
        open.retain(|_, v| v.strong_count() != 0);
        if let Some(q) = open.get(&dir).and_then(Weak::upgrade) {
            if q.spec != *spec || q.binding != binding {
                return Err(invalid("input DLQ directory binding differs"));
            }
            return Ok(q);
        }
        if open.len() >= 1024 {
            return Err(invalid("too many input DLQs"));
        }
        let lock = sparrow_io::fs_lock::FileLock::acquire(&dir.join("WRITER_LOCK"))?;
        for e in std::fs::read_dir(&dir).map_err(io)? {
            let e = e.map_err(io)?;
            let m = std::fs::symlink_metadata(e.path()).map_err(io)?;
            if !matches!(
                e.file_name().to_str(),
                Some("WRITER_LOCK" | "input.sqlite3" | "input.sqlite3-journal")
            ) || !m.is_file()
                || m.file_type().is_symlink()
                || m.len() > spec.max_disk_bytes / 2
            {
                return Err(invalid(
                    "input DLQ requires its own bounded directory without links or foreign files",
                ));
            }
        }
        let path = dir.join("input.sqlite3");
        let mut flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        if create {
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        }
        let mut conn = Connection::open_with_flags(&path, flags).map_err(db)?;
        conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-32; PRAGMA trusted_schema=OFF;").map_err(db)?;
        let page: u64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(db)?;
        let cap = (spec.max_disk_bytes / 2 - 65536) / page;
        let actual: u64 = conn
            .query_row(&format!("PRAGMA max_page_count={cap}"), [], |r| r.get(0))
            .map_err(db)?;
        if actual > cap {
            return Err(invalid("input DLQ page budget exceeded"));
        }
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='meta')",
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        let policy = serde_json::to_string(spec).map_err(|_| invalid("DLQ policy encoding"))?;
        if !exists {
            if !create {
                return Err(invalid("input DLQ metadata missing"));
            }
            let tx = conn.transaction().map_err(db)?;
            tx.execute_batch("CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,uuid TEXT NOT NULL,binding TEXT NOT NULL,policy TEXT NOT NULL,source TEXT,replay_floor INTEGER NOT NULL,captured INTEGER NOT NULL,purged INTEGER NOT NULL);
                CREATE TABLE records(position INTEGER PRIMARY KEY,record_index INTEGER NOT NULL,body BLOB NOT NULL,digest TEXT NOT NULL,code TEXT NOT NULL,at_ms INTEGER NOT NULL);
                CREATE TABLE audit(id INTEGER PRIMARY KEY AUTOINCREMENT,at_ms INTEGER NOT NULL,action TEXT NOT NULL,position INTEGER,detail TEXT NOT NULL);").map_err(db)?;
            tx.execute(
                "INSERT INTO meta VALUES(1,1,?1,?2,?3,NULL,0,0,0)",
                params![random_id()?, binding, policy],
            )
            .map_err(db)?;
            tx.commit().map_err(db)?;
            std::fs::File::open(&dir)
                .and_then(|f| f.sync_all())
                .map_err(io)?;
        }
        let (version, uuid, saved, policy_saved): (u32, String, String, String) = conn
            .query_row(
                "SELECT version,uuid,binding,policy FROM meta WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map_err(db)?;
        if version != 1
            || saved != binding
            || policy_saved != policy
            || uuid.len() != 32
            || !uuid.bytes().all(|c| c.is_ascii_hexdigit())
        {
            return Err(invalid("input DLQ identity/policy/version mismatch"));
        }
        let (count, bytes): (usize, u64) = conn
            .query_row(
                "SELECT count(*),coalesce(sum(length(body)),0) FROM records",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(db)?;
        if count > spec.max_entries || bytes > spec.max_payload_bytes {
            return Err(invalid("stored input DLQ exceeds limits"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(io)?;
        }
        let q = Arc::new(Self {
            conn: Mutex::new(conn),
            spec: spec.clone(),
            binding: binding.into(),
            uuid,
            _lock: lock,
        });
        open.insert(dir, Arc::downgrade(&q));
        Ok(q)
    }
    pub fn uuid(&self) -> &str {
        &self.uuid
    }
    fn tx<T>(&self, f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut c = self
            .conn
            .lock()
            .map_err(|_| invalid("input DLQ mutex poisoned"))?;
        let tx = c
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(db)?;
        let v = f(&tx)?;
        tx.commit().map_err(db)?;
        Ok(v)
    }
    fn audit(tx: &Connection, action: &str, position: u64, reason: &str) -> Result<()> {
        tx.execute(
            "INSERT INTO audit(at_ms,action,position,detail) VALUES(?1,?2,?3,?4)",
            params![
                sparrow_io::durable::wall_ms() as i64,
                action,
                position as i64,
                reason
            ],
        )
        .map_err(db)?;
        tx.execute(
            "DELETE FROM audit WHERE id NOT IN(SELECT id FROM audit ORDER BY id DESC LIMIT 200)",
            [],
        )
        .map_err(db)?;
        Ok(())
    }
    pub fn status(&self) -> Result<Value> {
        let c = self
            .conn
            .lock()
            .map_err(|_| invalid("input DLQ mutex poisoned"))?;
        let (count, bytes): (u64, u64) = c
            .query_row(
                "SELECT count(*),coalesce(sum(length(body)),0) FROM records",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(db)?;
        let (floor, captured, purged): (u64, u64, u64) = c
            .query_row(
                "SELECT replay_floor,captured,purged FROM meta WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(db)?;
        let mut q = c
            .prepare("SELECT at_ms,action,position,detail FROM audit ORDER BY id DESC LIMIT 32")
            .map_err(db)?;
        let audit=q.query_map([],|r|Ok(json!({"at_ms":r.get::<_,u64>(0)?,"action":r.get::<_,String>(1)?,"position":r.get::<_,u64>(2)?,"reason":r.get::<_,String>(3)?}))).map_err(db)?.collect::<std::result::Result<Vec<_>,_>>().map_err(db)?;
        Ok(
            json!({"uuid":self.uuid,"entries":count,"payload_bytes":bytes,"replay_floor":floor,"captured_total":captured,"purged_total":purged,"receipt":"local_FULL_commit_before_source_publication; ACK_still_requires_checkpoint","full_policy":"bounded_wait_keep_checkpoint_control_available_no_eviction","audit":audit}),
        )
    }
    pub fn entries(&self, after: u64, limit: usize) -> Result<Value> {
        if after > i64::MAX as u64 || !(1..=100).contains(&limit) {
            return Err(invalid("input DLQ page out of bounds"));
        }
        let c = self
            .conn
            .lock()
            .map_err(|_| invalid("input DLQ mutex poisoned"))?;
        let mut q=c.prepare("SELECT position,record_index,length(body),digest,code,at_ms FROM records WHERE position>?1 ORDER BY position LIMIT ?2").map_err(db)?;
        let rows=q.query_map(params![after as i64,limit as i64],|r|Ok(json!({"position":r.get::<_,u64>(0)?,"record_index":r.get::<_,u64>(1)?,"body_bytes":r.get::<_,u64>(2)?,"sha256":r.get::<_,String>(3)?,"code":r.get::<_,String>(4)?,"at_ms":r.get::<_,u64>(5)?}))).map_err(db)?.collect::<std::result::Result<Vec<_>,_>>().map_err(db)?;
        Ok(json!({"uuid":self.uuid,"entries":rows}))
    }
    pub fn body(&self, position: u64) -> Result<Vec<u8>> {
        if position == 0 || position > i64::MAX as u64 {
            return Err(invalid("invalid input DLQ position"));
        }
        let c = self
            .conn
            .lock()
            .map_err(|_| invalid("input DLQ mutex poisoned"))?;
        let len: u64 = c
            .query_row(
                "SELECT length(body) FROM records WHERE position=?1",
                [position as i64],
                |r| r.get(0),
            )
            .map_err(db)?;
        if len > 65536 {
            return Err(invalid("stored input DLQ body exceeds bound"));
        }
        let (body, digest): (Vec<u8>, String) = c
            .query_row(
                "SELECT body,digest FROM records WHERE position=?1",
                [position as i64],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(db)?;
        if hash(&body) != digest {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "input DLQ digest mismatch",
            ));
        }
        Ok(body)
    }
    pub fn entry(&self, position: u64) -> Result<Value> {
        use base64::Engine;
        Ok(
            json!({"uuid":self.uuid,"position":position,"body_base64":base64::engine::general_purpose::STANDARD.encode(self.body(position)?)}),
        )
    }
    /// Only the stopped-pipeline control operation may authorize this using
    /// an independently decoded committed checkpoint. No pending input purge.
    pub(crate) fn purge(
        &self,
        uuid: &str,
        position: u64,
        committed: &SourcePosition,
        approve_floor: u64,
        reason: &str,
    ) -> Result<()> {
        audit_reason(reason)?;
        if uuid != self.uuid
            || approve_floor != position
            || position == 0
            || position > committed.offset_bytes
        {
            return Err(invalid("purge requires matching UUID, exact replay-floor approval and a committed source cut"));
        }
        self.check_start(committed)?;
        self.tx(|tx| {
            if tx
                .execute("DELETE FROM records WHERE position=?1", [position as i64])
                .map_err(db)?
                != 1
            {
                return Err(invalid("input DLQ record does not exist"));
            }
            tx.execute(
                "UPDATE meta SET replay_floor=max(replay_floor,?1),purged=purged+1 WHERE id=1",
                [position as i64],
            )
            .map_err(db)?;
            Self::audit(tx, "purge_and_retire_replay_prefix", position, reason)
        })
    }
}
pub(crate) fn audit_reason(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 256 || s.chars().any(char::is_control) {
        Err(invalid(
            "audit reason must be 1..256 bytes without control characters",
        ))
    } else {
        Ok(())
    }
}
pub(crate) fn separate_storage(spec: &crate::PipelineSpec, artifact: Option<&str>) -> Result<()> {
    let mut dirs = Vec::new();
    for path in [
        spec.checkpoint_dir.as_deref(),
        spec.source.input_dlq.as_ref().map(|q| q.directory.as_str()),
        spec.sink
            .durable_outbox
            .as_ref()
            .map(|q| q.directory.as_str()),
        artifact,
    ]
    .into_iter()
    .flatten()
    {
        let resolved = sparrow_connectors::policy::checked_data_path_identity(Path::new(path))
            .map_err(SparrowError::from)?;
        if dirs
            .iter()
            .any(|old: &PathBuf| old.starts_with(&resolved) || resolved.starts_with(old))
        {
            return Err(invalid("checkpoint, input DLQ, output outbox and replay artifacts require separate non-overlapping directories"));
        }
        dirs.push(resolved);
    }
    Ok(())
}
pub(crate) fn check_update(old: &crate::PipelineSpec, new: &crate::PipelineSpec) -> Result<()> {
    if old.source.input_dlq != new.source.input_dlq
        || (old.source.input_dlq.is_some()
            && (old.source != new.source
                || old.stream != new.stream
                || old.checkpoint_dir != new.checkpoint_dir
                || old.recovery != new.recovery))
    {
        return Err(invalid(
            "input DLQ/source/checkpoint binding is immutable; use an explicit new lineage",
        ));
    }
    Ok(())
}
fn binding(store: &crate::Store, name: &str, spec: &crate::PipelineSpec) -> Result<String> {
    let namespace = store
        .existing_outbox_namespace()?
        .ok_or_else(|| invalid("input DLQ catalog namespace missing"))?;
    let schema = store.get_stream(&spec.stream)?.schema_json;
    serde_json::to_string(&json!({"catalog":namespace,"name":name,"source":spec.source,"schema":schema,"checkpoint_dir":spec.checkpoint_dir}))
        .map_err(|_|invalid("input DLQ binding encoding"))
}
pub(crate) fn initialize(
    store: &crate::Store,
    name: &str,
    spec: &crate::PipelineSpec,
) -> Result<Arc<InputDlq>> {
    let config = spec
        .source
        .input_dlq
        .as_ref()
        .ok_or_else(|| invalid("input DLQ not configured"))?;
    let expected = store.input_dlq_identity(name)?;
    if expected.is_none() {
        let dir = spec
            .checkpoint_dir
            .as_ref()
            .ok_or_else(|| invalid("input DLQ needs an explicit fresh checkpoint directory"))?;
        sparrow_connectors::check_data_path(Path::new(dir)).map_err(SparrowError::from)?;
        if Path::new(dir).try_exists().map_err(io)?
            && std::fs::read_dir(dir).map_err(io)?.next().is_some()
        {
            return Err(invalid(
                "new input DLQ cannot adopt pre-existing checkpoint history",
            ));
        }
    }
    store.outbox_namespace()?;
    let queue = InputDlq::open(config, &binding(store, name, spec)?, expected.is_none())?;
    if expected.as_ref().is_some_and(|saved| saved != queue.uuid()) {
        return Err(invalid("input DLQ UUID differs from catalog"));
    }
    store.pin_input_dlq_identity(name, queue.uuid())?;
    Ok(queue)
}
pub fn configured(store: &crate::Store, name: &str) -> Result<Arc<InputDlq>> {
    let row = store.get_pipeline(name)?;
    let config = row
        .spec
        .source
        .input_dlq
        .as_ref()
        .ok_or_else(|| invalid("pipeline has no input DLQ"))?;
    let expected = store
        .input_dlq_identity(name)?
        .ok_or_else(|| invalid("input DLQ not initialized"))?;
    let queue = InputDlq::open(config, &binding(store, name, &row.spec)?, false)?;
    if queue.uuid() != expected {
        return Err(invalid("input DLQ UUID differs from catalog"));
    }
    Ok(queue)
}
impl InputQuarantine for InputDlq {
    fn check_start(&self, p: &SourcePosition) -> Result<()> {
        let identity =
            serde_json::to_string(&json!({"kind":p.identity.kind,"path":p.identity.path}))
                .map_err(|_| invalid("source identity encoding"))?;
        self.tx(|tx| {
            let (saved,floor):(Option<String>,u64)=tx.query_row("SELECT source,replay_floor FROM meta WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?))).map_err(db)?;
            if saved.as_ref().is_some_and(|s|s!=&identity) || p.offset_bytes<floor {return Err(SparrowError::new(ErrorCode::UnsupportedRestore,"input DLQ source incarnation differs or checkpoint precedes retired replay floor"));}
            if saved.is_none(){tx.execute("UPDATE meta SET source=?1 WHERE id=1",[identity]).map_err(db)?;}
            Ok(())
        })
    }
    fn capture(&self, p: &SourcePosition, payload: &[u8], code: ErrorCode) -> Result<()> {
        if !sparrow_io::poison::record_error(code)
            || payload.len() > 65536
            || p.offset_bytes == 0
            || p.offset_bytes > i64::MAX as u64
            || p.record_index > i64::MAX as u64
        {
            return Err(invalid("record cannot be safely quarantined"));
        }
        self.check_start(p)?;
        let digest = hash(payload);
        self.tx(|tx| {
            let old: Option<(String, u64)> = tx
                .query_row(
                    "SELECT digest,length(body) FROM records WHERE position=?1",
                    [p.offset_bytes as i64],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(db)?;
            if let Some((old, len)) = old {
                if old != digest || len > 65536 {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "same source position has different poison payload",
                    ));
                }
                let stored: Vec<u8> = tx
                    .query_row(
                        "SELECT body FROM records WHERE position=?1",
                        [p.offset_bytes as i64],
                        |r| r.get(0),
                    )
                    .map_err(db)?;
                if stored.as_slice() != payload {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "stored input quarantine body differs; refusing dedup acknowledgement",
                    ));
                }
                return Ok(());
            }
            let (count, bytes): (usize, u64) = tx
                .query_row(
                    "SELECT count(*),coalesce(sum(length(body)),0) FROM records",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(db)?;
            if count >= self.spec.max_entries
                || bytes.saturating_add(payload.len() as u64) > self.spec.max_payload_bytes
            {
                return Err(SparrowError::new(
                    ErrorCode::ResourceExhausted,
                    "input DLQ full; refusing to skip or ACK",
                )
                .context("input_dlq_full", "true"));
            }
            tx.execute(
                "INSERT INTO records VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    p.offset_bytes as i64,
                    p.record_index as i64,
                    payload,
                    digest,
                    code.as_str(),
                    sparrow_io::durable::wall_ms() as i64
                ],
            )
            .map_err(db)?;
            tx.execute("UPDATE meta SET captured=captured+1 WHERE id=1", [])
                .map_err(db)?;
            Self::audit(tx, "quarantine", p.offset_bytes, code.as_str())
        })
    }
}

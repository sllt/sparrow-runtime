//! Bounded durable HTTP outbox, separate from the catalog and window codecs.
//! SQLite DELETE/FULL transactions provide the local acceptance point. No
//! pending/DLQ record is automatically expired or silently evicted.
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sparrow_io::durable::{DurableHttpQueue, DurableOutcome, DurableRequest};
use sparrow_model::{CreditKind, ErrorCode, MemoryOwner, Result, SparrowError};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxSpec {
    pub directory: String,
    /// Physical bound includes a full rollback journal, plus file slack.
    pub max_disk_bytes: u64,
    pub max_pending_bytes: u64,
    pub max_dlq_bytes: u64,
    pub max_pending_entries: usize,
    pub max_dlq_entries: usize,
    pub max_record_bytes: usize,
    pub max_attempts: u32,
    pub max_retry_elapsed_ms: u64,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
}

fn err(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message).context("component", "durable_http_outbox")
}
fn invalid(message: &str) -> SparrowError {
    err(ErrorCode::InvalidArgument, message)
}
fn db(e: rusqlite::Error) -> SparrowError {
    err(ErrorCode::Internal, format!("outbox database: {e}"))
}
fn io(e: std::io::Error) -> SparrowError {
    err(ErrorCode::Internal, format!("outbox filesystem: {e}"))
}
fn hash(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect()
}
fn uuid() -> Result<String> {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| err(ErrorCode::Internal, "outbox entropy unavailable"))?;
    Ok(bytes.iter().map(|v| format!("{v:02x}")).collect())
}

impl OutboxSpec {
    pub fn validate(&self) -> Result<()> {
        if self.directory.is_empty()
            || self.directory.len() > 4096
            || !Path::new(&self.directory).is_absolute()
            || !(8 * 1024 * 1024..=4 * 1024 * 1024 * 1024u64).contains(&self.max_disk_bytes)
            || !(1..=10_000).contains(&self.max_pending_entries)
            || !(1..=10_000).contains(&self.max_dlq_entries)
            || !(2..=1024 * 1024).contains(&self.max_record_bytes)
            || self.max_pending_bytes < self.max_record_bytes as u64
            || self.max_dlq_bytes < self.max_record_bytes as u64
            || self.max_pending_bytes.saturating_add(self.max_dlq_bytes) > self.max_disk_bytes / 4
            || !(1..=100).contains(&self.max_attempts)
            || !(1000..=7 * 24 * 3600 * 1000).contains(&self.max_retry_elapsed_ms)
            || !(25..=60_000).contains(&self.retry_base_ms)
            || self.retry_max_ms < self.retry_base_ms
            || self.retry_max_ms > 3_600_000
        {
            return Err(invalid("invalid durable outbox limits: absolute directory, disk 8MiB..4GiB, total payload <= disk/4, bounded entries/body/retries required"));
        }
        sparrow_connectors::check_data_path(Path::new(&self.directory))
            .map_err(SparrowError::from)?;
        Ok(())
    }
    fn backoff(&self, attempts: u32) -> u64 {
        self.retry_base_ms
            .saturating_mul(1u64 << attempts.saturating_sub(1).min(30))
            .min(self.retry_max_ms)
    }
}

struct Inner {
    conn: Connection,
    uuid: String,
}
pub struct Outbox {
    inner: Mutex<Inner>,
    spec: OutboxSpec,
    binding: String,
    worker: AtomicBool,
    _lock: sparrow_io::fs_lock::FileLock,
}
static OPEN: OnceLock<Mutex<HashMap<PathBuf, Weak<Outbox>>>> = OnceLock::new();

impl Outbox {
    pub fn identity(&self) -> Result<String> {
        self.inner
            .lock()
            .map(|inner| inner.uuid.clone())
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))
    }
    pub fn status(&self) -> Result<Value> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))?;
        let (pending, pending_bytes) = Self::totals(&inner.conn, "pending")?;
        let (dlq, dlq_bytes) = Self::totals(&inner.conn, "dlq")?;
        let (blocked, _) = Self::totals(&inner.conn, "blocked")?;
        let (paused, accepted, delivered, dead, replayed, purged): (bool, u64, u64, u64, u64, u64) =
            inner
                .conn
                .query_row(
                    "SELECT paused,accepted,delivered,dead,replayed,purged FROM meta WHERE id=1",
                    [],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get(3)?,
                            r.get(4)?,
                            r.get(5)?,
                        ))
                    },
                )
                .map_err(db)?;
        let oldest: Option<u64> = inner
            .conn
            .query_row(
                "SELECT min(created_ms) FROM entries WHERE state='pending'",
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        let pages: u64 = inner
            .conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .map_err(db)?;
        let page: u64 = inner
            .conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(db)?;
        let mut stmt = inner
            .conn
            .prepare("SELECT at_ms,action,entry,detail FROM audit ORDER BY id DESC LIMIT 32")
            .map_err(db)?;
        let audit=stmt.query_map([],|r|Ok(json!({"at_ms":r.get::<_,i64>(0)?,"action":r.get::<_,String>(1)?,"entry":r.get::<_,Option<i64>>(2)?,"detail":r.get::<_,String>(3)?}))).map_err(db)?
            .collect::<std::result::Result<Vec<_>,_>>().map_err(db)?;
        Ok(
            json!({"uuid":inner.uuid,"receipt":"local_sqlite_full_commit","remote_delivery":"at_least_once",
            "paused":paused,"sender_active":self.worker.load(Ordering::Acquire),"pending_entries":pending,"pending_bytes":pending_bytes,
            "dlq_entries":dlq,"dlq_bytes":dlq_bytes,"blocked_for_dlq_space":blocked,"oldest_pending_created_ms":oldest,"database_bytes":pages*page,"max_disk_bytes":self.spec.max_disk_bytes,
            "accepted_total":accepted,"delivered_total":delivered,"dead_total":dead,"replayed_total":replayed,"purged_total":purged,
            "id_contract":"stable_per_persisted_request; source_replay_may_create_another_request; JetStream_row_output_ids_preserved",
            "full_policy":"fail_closed_keep_records","retention":"no_automatic_eviction_explicit_authenticated_dlq_purge_only","audit":audit}),
        )
    }
    pub fn entries(&self, state: &str, after: u64, limit: usize) -> Result<Value> {
        if !matches!(state, "pending" | "dlq" | "blocked")
            || !(1..=100).contains(&limit)
            || after > i64::MAX as u64
        {
            return Err(invalid("invalid outbox page"));
        }
        let inner = self
            .inner
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))?;
        let mut stmt=inner.conn.prepare("SELECT id,length(body),digest,created_ms,attempts,next_ms,reason,replay_generation FROM entries WHERE state=?1 AND id>?2 ORDER BY id LIMIT ?3").map_err(db)?;
        let rows=stmt.query_map(params![state,after as i64,limit as i64],|r|{
            let id:i64=r.get(0)?;
            Ok(json!({"id":format!("{}-{id}",inner.uuid),"sequence":id,"state":state,"body_bytes":r.get::<_,u64>(1)?,"sha256":r.get::<_,String>(2)?,
                "created_ms":r.get::<_,u64>(3)?,"attempts":r.get::<_,u32>(4)?,"next_ms":r.get::<_,u64>(5)?,"reason":r.get::<_,String>(6)?,"replay_generation":r.get::<_,u64>(7)?}))
        }).map_err(db)?.collect::<std::result::Result<Vec<_>,_>>().map_err(db)?;
        Ok(json!({"state":state,"entries":rows,"limit":limit,"body_included":false}))
    }
    pub fn entry(&self, key: &str) -> Result<Value> {
        use base64::Engine;
        let inner = self
            .inner
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))?;
        let id = parse_id(&inner.uuid, key)?;
        let len: usize = inner
            .conn
            .query_row("SELECT length(body) FROM entries WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .map_err(db)?;
        if len > self.spec.max_record_bytes {
            return Err(err(ErrorCode::BoundExceeded, "stored body exceeds limit"));
        }
        let (body, digest, state): (Vec<u8>, String, String) = inner
            .conn
            .query_row(
                "SELECT body,digest,state FROM entries WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(db)?;
        if hash(&body) != digest {
            return Err(err(
                ErrorCode::CodecViolation,
                "outbox body digest mismatch",
            ));
        }
        Ok(
            json!({"id":key,"state":state,"sha256":digest,"body_base64":base64::engine::general_purpose::STANDARD.encode(body)}),
        )
    }
    pub fn command(&self, request: &OutboxCommand, now: u64) -> Result<Value> {
        if request.reason.is_empty()
            || request.reason.len() > 256
            || request.reason.chars().any(char::is_control)
        {
            return Err(invalid("an audit reason of 1..256 characters is required"));
        }
        self.transaction(|tx,uuid|{
            if request.approve_uuid!=uuid{return Err(invalid("approve_uuid must match the current outbox"));}
            match request.action.as_str(){
                "pause"|"resume"=>{
                    if request.id.is_some()||request.replay_generation.is_some(){return Err(invalid("pause/resume do not accept an entry"));}
                    tx.execute("UPDATE meta SET paused=?1 WHERE id=1",[request.action=="pause"]).map_err(db)?;
                    Self::audit(tx,now,&request.action,None,&request.reason)?;
                }
                "replay"|"purge"=>{
                    let key=request.id.as_deref().ok_or_else(||invalid("an exact DLQ entry id is required"))?;
                    let id=parse_id(uuid,key)?;
                    let (state,len,generation):(String,usize,u64)=tx.query_row("SELECT state,length(body),replay_generation FROM entries WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(db)?;
                    if state!="dlq" || request.replay_generation!=Some(generation){return Err(invalid("operation requires a current DLQ entry and matching replay_generation"));}
                    if request.action=="replay"{
                        if generation>=1000 {return Err(err(ErrorCode::BoundExceeded,"manual replay generation limit reached"));}
                        self.capacity(tx,"pending",len)?;
                        tx.execute("UPDATE entries SET state='pending',attempts=0,created_ms=?2,next_ms=?2,reason='',replay_generation=replay_generation+1 WHERE id=?1",params![id,now as i64]).map_err(db)?;
                        tx.execute("UPDATE meta SET replayed=replayed+1 WHERE id=1",[]).map_err(db)?;
                    }else{
                        tx.execute("DELETE FROM entries WHERE id=?1",[id]).map_err(db)?;
                        tx.execute("UPDATE meta SET purged=purged+1 WHERE id=1",[]).map_err(db)?;
                    }
                    Self::audit(tx,now,&request.action,Some(id),&request.reason)?;
                }
                _=>return Err(invalid("outbox action must be pause, resume, replay or purge")),
            }
            Ok(json!({"applied":true,"action":request.action,"id":request.id,"audit":"committed_atomically_in_outbox"}))
        })
    }

    pub fn check_resume(&self, request: &OutboxCommand) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))?;
        if request.action != "resume"
            || request.approve_uuid != inner.uuid
            || request.id.is_some()
            || request.replay_generation.is_some()
            || request.reason.is_empty()
            || request.reason.len() > 256
            || request.reason.chars().any(char::is_control)
        {
            return Err(invalid(
                "resume requires matching UUID, an audit reason and no entry selector",
            ));
        }
        Ok(())
    }
    /// Open/create only a configured, allowlisted directory. The metadata
    /// permanently binds its catalog/pipeline, target, encoding and policy.
    pub fn open(spec: &OutboxSpec, binding: &str, create: bool) -> Result<Arc<Self>> {
        spec.validate()?;
        if create {
            std::fs::create_dir_all(&spec.directory).map_err(io)?;
        }
        let dir = std::fs::canonicalize(&spec.directory).map_err(io)?;
        sparrow_connectors::check_data_path(&dir).map_err(SparrowError::from)?;
        let mut registry = OPEN
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox registry poisoned"))?;
        registry.retain(|_, v| v.strong_count() != 0);
        if let Some(open) = registry.get(&dir).and_then(Weak::upgrade) {
            if open.spec != *spec || open.binding != binding {
                return Err(invalid(
                    "outbox directory is bound to a different pipeline/target/policy",
                ));
            }
            return Ok(open);
        }
        if registry.len() >= 1024 {
            return Err(err(ErrorCode::ResourceExhausted, "too many open outboxes"));
        }
        let lock = sparrow_io::fs_lock::FileLock::acquire(&dir.join("WRITER_LOCK"))?;
        // Inspect sidecars only with the writer lock and no active connection:
        // a live SQLite transaction can legitimately create/remove its journal.
        for entry in std::fs::read_dir(&dir).map_err(io)? {
            let entry = entry.map_err(io)?;
            let name = entry.file_name();
            let allowed = matches!(
                name.to_str(),
                Some(
                    "WRITER_LOCK"
                        | "outbox.sqlite3"
                        | "outbox.sqlite3-journal"
                        | "outbox.sqlite3-wal"
                        | "outbox.sqlite3-shm"
                )
            );
            let meta = std::fs::symlink_metadata(entry.path()).map_err(io)?;
            if !allowed
                || !meta.is_file()
                || meta.file_type().is_symlink()
                || meta.len() > spec.max_disk_bytes
            {
                return Err(invalid("outbox requires its own directory with bounded regular SQLite files, no links or foreign artifacts"));
            }
        }
        let path = dir.join("outbox.sqlite3");
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if !meta.is_file()
                || meta.file_type().is_symlink()
                || meta.len() > spec.max_disk_bytes / 2
            {
                return Err(invalid("outbox database must be a bounded regular file"));
            }
        } else if !create {
            return Err(invalid("outbox has not been initialized"));
        }
        let mut flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        if create {
            flags |= rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
        }
        let conn = Connection::open_with_flags(&path, flags).map_err(db)?;
        conn.busy_timeout(std::time::Duration::from_millis(100))
            .map_err(db)?;
        conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-32; PRAGMA trusted_schema=OFF;").map_err(db)?;
        let page_size: u64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(db)?;
        let page_cap = (spec.max_disk_bytes / 2 - 65536) / page_size;
        let actual: u64 = conn
            .query_row(&format!("PRAGMA max_page_count={page_cap}"), [], |r| {
                r.get(0)
            })
            .map_err(db)?;
        if actual > page_cap {
            return Err(err(
                ErrorCode::BoundExceeded,
                "outbox database already exceeds its page budget",
            ));
        }
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta')",
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        if !exists {
            if !create {
                return Err(invalid("outbox metadata is missing"));
            }
            let id = uuid()?;
            conn.execute_batch("BEGIN IMMEDIATE; CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, uuid TEXT NOT NULL, binding TEXT NOT NULL, policy TEXT NOT NULL, next_id INTEGER NOT NULL, paused INTEGER NOT NULL, accepted INTEGER NOT NULL, delivered INTEGER NOT NULL, dead INTEGER NOT NULL, replayed INTEGER NOT NULL, purged INTEGER NOT NULL);
                CREATE TABLE entries(id INTEGER PRIMARY KEY, state TEXT NOT NULL CHECK(state IN ('pending','dlq','blocked')), body BLOB NOT NULL, digest TEXT NOT NULL, created_ms INTEGER NOT NULL, attempts INTEGER NOT NULL, next_ms INTEGER NOT NULL, reason TEXT NOT NULL, replay_generation INTEGER NOT NULL);
                CREATE INDEX dispatch ON entries(state,id);
                CREATE TABLE audit(id INTEGER PRIMARY KEY AUTOINCREMENT, at_ms INTEGER NOT NULL, action TEXT NOT NULL, entry INTEGER, detail TEXT NOT NULL);").map_err(db)?;
            let policy =
                serde_json::to_string(spec).map_err(|_| invalid("outbox policy encoding"))?;
            conn.execute(
                "INSERT INTO meta VALUES(1,1,?1,?2,?3,1,0,0,0,0,0,0)",
                params![id, binding, policy],
            )
            .map_err(db)?;
            conn.execute_batch("PRAGMA user_version=1; COMMIT;")
                .map_err(db)?;
            std::fs::File::open(&dir)
                .and_then(|f| f.sync_all())
                .map_err(io)?;
        }
        let (version, id, saved, policy): (u32, String, String, String) = conn
            .query_row(
                "SELECT version,uuid,binding,policy FROM meta WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map_err(db)?;
        let want_policy =
            serde_json::to_string(spec).map_err(|_| invalid("outbox policy encoding"))?;
        if version != 1
            || saved != binding
            || policy != want_policy
            || id.len() != 32
            || !id.bytes().all(|c| c.is_ascii_hexdigit())
        {
            return Err(invalid("outbox version, pipeline, target or retry policy mismatch; existing output cannot be redirected"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(io)?;
        }
        let out = Arc::new(Self {
            inner: Mutex::new(Inner { conn, uuid: id }),
            spec: spec.clone(),
            binding: binding.into(),
            worker: AtomicBool::new(false),
            _lock: lock,
        });
        out.check_bounds()?;
        registry.insert(dir, Arc::downgrade(&out));
        Ok(out)
    }

    fn transaction<T>(
        &self,
        f: impl FnOnce(&rusqlite::Transaction<'_>, &str) -> Result<T>,
    ) -> Result<T> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))?;
        let id = inner.uuid.clone();
        let tx = inner
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(db)?;
        let result = f(&tx, &id)?;
        tx.commit().map_err(db)?;
        Ok(result)
    }
    fn totals(c: &Connection, state: &str) -> Result<(u64, u64)> {
        c.query_row("SELECT count(*),coalesce(sum(length(body)),0) FROM entries WHERE state=?1 OR (?1='pending' AND state='blocked')", [state], |r|Ok((r.get(0)?,r.get(1)?))).map_err(db)
    }
    fn check_bounds(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox mutex poisoned"))?;
        for (state, count_cap, byte_cap) in [
            (
                "pending",
                self.spec.max_pending_entries,
                self.spec.max_pending_bytes,
            ),
            ("dlq", self.spec.max_dlq_entries, self.spec.max_dlq_bytes),
        ] {
            let (n, bytes) = Self::totals(&inner.conn, state)?;
            if n > count_cap as u64 || bytes > byte_cap {
                return Err(err(
                    ErrorCode::BoundExceeded,
                    "stored outbox exceeds configured limits",
                ));
            }
        }
        let oversized: bool = inner.conn.query_row("SELECT EXISTS(SELECT 1 FROM entries WHERE length(body)>?1 OR id<=0 OR attempts<0 OR created_ms<0 OR next_ms<0)", [self.spec.max_record_bytes as i64], |r|r.get(0)).map_err(db)?;
        if oversized {
            return Err(err(
                ErrorCode::CodecViolation,
                "invalid outbox record metadata",
            ));
        }
        Ok(())
    }
    fn capacity(&self, tx: &Connection, state: &str, add: usize) -> Result<()> {
        let (n, bytes) = Self::totals(tx, state)?;
        let (nc, bc) = if state == "pending" {
            (self.spec.max_pending_entries, self.spec.max_pending_bytes)
        } else {
            (self.spec.max_dlq_entries, self.spec.max_dlq_bytes)
        };
        if n >= nc as u64 || bytes.saturating_add(add as u64) > bc {
            return Err(err(
                ErrorCode::ResourceExhausted,
                format!("{state} outbox capacity exhausted; records are retained"),
            ));
        }
        Ok(())
    }
    fn audit(tx: &Connection, now: u64, action: &str, id: Option<i64>, detail: &str) -> Result<()> {
        tx.execute(
            "INSERT INTO audit(at_ms,action,entry,detail) VALUES(?1,?2,?3,?4)",
            params![now as i64, action, id, detail],
        )
        .map_err(db)?;
        tx.execute(
            "DELETE FROM audit WHERE id NOT IN (SELECT id FROM audit ORDER BY id DESC LIMIT 200)",
            [],
        )
        .map_err(db)?;
        Ok(())
    }
    fn dead(&self, tx: &Connection, id: i64, bytes: usize, now: u64, reason: &str) -> Result<()> {
        if let Err(error) = self.capacity(tx, "dlq", bytes) {
            if error.code != ErrorCode::ResourceExhausted {
                return Err(error);
            }
            // Terminal failure is durable even when the DLQ has no room.
            // Never POST this record again until an operator frees DLQ space.
            tx.execute(
                "UPDATE entries SET state='blocked',reason=?2 WHERE id=?1",
                params![id, reason],
            )
            .map_err(db)?;
            return Ok(());
        }
        tx.execute(
            "UPDATE entries SET state='dlq',reason=?2 WHERE id=?1",
            params![id, reason],
        )
        .map_err(db)?;
        tx.execute("UPDATE meta SET dead=dead+1 WHERE id=1", [])
            .map_err(db)?;
        Self::audit(tx, now, "dead", Some(id), reason)
    }
    pub fn attach(self: &Arc<Self>) -> Result<Arc<dyn DurableHttpQueue>> {
        self.worker
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                err(
                    ErrorCode::ResourceExhausted,
                    "outbox already has an active sender",
                )
            })?;
        Ok(Arc::new(BoundQueue(self.clone())))
    }
    fn enqueue(&self, body: &[u8], now: u64) -> Result<String> {
        if body.len() > self.spec.max_record_bytes || body.is_empty() {
            return Err(err(
                ErrorCode::BoundExceeded,
                "outbox record exceeds byte limit",
            ));
        }
        self.transaction(|tx, uuid| {
            self.capacity(tx, "pending", body.len())?;
            let id: i64 = tx
                .query_row("SELECT next_id FROM meta WHERE id=1", [], |r| r.get(0))
                .map_err(db)?;
            if id <= 0 || id == i64::MAX {
                return Err(err(ErrorCode::BoundExceeded, "outbox sequence exhausted"));
            }
            tx.execute(
                "INSERT INTO entries VALUES(?1,'pending',?2,?3,?4,0,?4,'',0)",
                params![id, body, hash(body), now as i64],
            )
            .map_err(db)?;
            tx.execute(
                "UPDATE meta SET next_id=next_id+1,accepted=accepted+1 WHERE id=1",
                [],
            )
            .map_err(db)?;
            Ok(format!("{uuid}-{id}"))
        })
    }
    fn next(&self, now: u64, owner: &Arc<MemoryOwner>) -> Result<Option<DurableRequest>> {
        self.transaction(|tx,uuid| {
            if tx.query_row("SELECT paused FROM meta WHERE id=1",[],|r|r.get::<_,bool>(0)).map_err(db)? {return Ok(None);}
            let row: Option<(i64,usize,u64,u32,u64,String,String)> = tx.query_row("SELECT id,length(body),created_ms,attempts,next_ms,state,reason FROM entries WHERE state!='dlq' ORDER BY id LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional().map_err(db)?;
            let Some((id,len,created,attempts,next,state,reason))=row else{return Ok(None)};
            if state=="blocked" {self.dead(tx,id,len,now,&reason)?;return Ok(None);}
            if now<created {return Err(invalid("wall clock moved before outbox creation time; refusing to reset retry budgets"));}
            if attempts>=self.spec.max_attempts || now-created>=self.spec.max_retry_elapsed_ms {
                self.dead(tx,id,len,now,"retry_budget_exhausted")?;
                return Ok(None);
            }
            if now<next {return Ok(None);}
            if len>self.spec.max_record_bytes {return Err(err(ErrorCode::CodecViolation,"oversized outbox body"));}
            let credit=owner.acquire(CreditKind::Reservation,len.saturating_mul(3).saturating_add(65536))?;
            let (body,digest):(Vec<u8>,String)=tx.query_row("SELECT body,digest FROM entries WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(db)?;
            if hash(&body)!=digest {return Err(err(ErrorCode::CodecViolation,"outbox body digest mismatch"));}
            let attempt=attempts+1;
            let due=now.saturating_add(self.spec.backoff(attempt));
            tx.execute("UPDATE entries SET attempts=?2,next_ms=?3 WHERE id=?1",params![id,attempt,due as i64]).map_err(db)?;
            Ok(Some(DurableRequest{id:format!("{uuid}-{id}"),attempt,body,credit}))
        })
    }
    fn settle(&self, key: &str, attempt: u32, outcome: DurableOutcome, now: u64) -> Result<()> {
        self.transaction(|tx,uuid| {
            let id=parse_id(uuid,key)?;
            let (saved,len,created):(u32,usize,u64)=tx.query_row("SELECT attempts,length(body),created_ms FROM entries WHERE id=?1 AND state='pending'",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(db)?;
            if saved!=attempt {return Err(invalid("stale outbox completion"));}
            match outcome {
                DurableOutcome::Delivered=>{
                    tx.execute("DELETE FROM entries WHERE id=?1",[id]).map_err(db)?;
                    tx.execute("UPDATE meta SET delivered=delivered+1 WHERE id=1",[]).map_err(db)?;
                }
                DurableOutcome::Dead{reason}=>self.dead(tx,id,len,now,reason)?,
                DurableOutcome::Retry{reason,retry_after_ms}=>{
                    if attempt>=self.spec.max_attempts || now.saturating_sub(created)>=self.spec.max_retry_elapsed_ms {
                        self.dead(tx,id,len,now,"retry_budget_exhausted")?;
                    }else{
                        let delay=self.spec.backoff(attempt).max(retry_after_ms.unwrap_or(0).min(self.spec.retry_max_ms));
                        tx.execute("UPDATE entries SET next_ms=?2,reason=?3 WHERE id=?1",params![id,now.saturating_add(delay) as i64,reason]).map_err(db)?;
                    }
                }
            }
            Ok(())
        })
    }

    /// Fast path for a Job writer after the control plane has prepared the
    /// queue/sender before source activation. No disk open on a runtime worker.
    pub fn prepared(spec: &OutboxSpec) -> Result<Arc<Self>> {
        let path = std::fs::canonicalize(&spec.directory).map_err(io)?;
        OPEN.get_or_init(Default::default)
            .lock()
            .map_err(|_| err(ErrorCode::Internal, "outbox registry poisoned"))?
            .get(&path)
            .and_then(Weak::upgrade)
            .filter(|o| o.spec == *spec)
            .ok_or_else(|| {
                err(
                    ErrorCode::Internal,
                    "outbox was not prepared before ingress",
                )
            })
    }

    pub fn sender_failed(&self, code: ErrorCode) {
        let _ = self.transaction(|tx, _| {
            Self::audit(
                tx,
                sparrow_io::durable::wall_ms(),
                "sender_failed",
                None,
                code.as_str(),
            )
        });
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxCommand {
    pub action: String,
    pub approve_uuid: String,
    pub reason: String,
    pub id: Option<String>,
    pub replay_generation: Option<u64>,
}

pub fn binding(namespace: &str, name: &str, sink: &crate::spec::SinkSpec) -> Result<String> {
    let cfg = crate::validate::http_config(sink, None)?;
    serde_json::to_string(&json!({"version":1,"catalog":namespace,"pipeline":name,
        "url":cfg.url,"secret_ref":cfg.header_secret,"format":"json-output-v1"}))
    .map_err(|_| invalid("outbox binding encoding"))
}

pub fn configured(store: &crate::Store, name: &str) -> Result<Arc<Outbox>> {
    let row = store.get_pipeline(name)?;
    let config = row
        .spec
        .sink
        .durable_outbox
        .as_ref()
        .ok_or_else(|| invalid("pipeline has no durable outbox"))?;
    let namespace = store
        .existing_outbox_namespace()?
        .ok_or_else(|| invalid("outbox has not been initialized; start the pipeline first"))?;
    let expected = store
        .outbox_identity(name)?
        .ok_or_else(|| invalid("outbox identity is not initialized; start the pipeline first"))?;
    let queue = Outbox::open(config, &binding(&namespace, name, &row.spec.sink)?, false)?;
    if queue.identity()? != expected {
        return Err(err(
            ErrorCode::CodecViolation,
            "outbox UUID does not match the catalog",
        ));
    }
    Ok(queue)
}

/// Establish the backup dependency before source activation. A registered
/// queue is never recreated, even if its files have disappeared. An old
/// checkpoint cannot be adopted by a newly created durable-output pipeline.
pub(crate) fn initialize(
    store: &crate::Store,
    name: &str,
    spec: &crate::spec::PipelineSpec,
) -> Result<Arc<Outbox>> {
    let config = spec
        .sink
        .durable_outbox
        .as_ref()
        .ok_or_else(|| invalid("missing durable outbox"))?;
    let expected = store.outbox_identity(name)?;
    if expected.is_none() {
        if let Some(dir) = &spec.checkpoint_dir {
            let dir = Path::new(dir);
            sparrow_connectors::check_data_path(dir).map_err(SparrowError::from)?;
            if dir.try_exists().map_err(io)? && std::fs::read_dir(dir).map_err(io)?.next().is_some()
            {
                return Err(invalid("a new durable outbox requires a fresh checkpoint directory; existing checkpoints may depend on different output storage"));
            }
        }
    }
    let namespace = store.outbox_namespace()?;
    let queue = Outbox::open(
        config,
        &binding(&namespace, name, &spec.sink)?,
        expected.is_none(),
    )?;
    let id = queue.identity()?;
    if expected.as_ref().is_some_and(|saved| saved != &id) {
        return Err(err(
            ErrorCode::CodecViolation,
            "outbox UUID does not match the catalog",
        ));
    }
    store.pin_outbox_identity(name, &id)?;
    Ok(queue)
}

pub(crate) fn check_retire(
    spec: &crate::spec::PipelineSpec,
    name: &str,
    namespace: Option<&str>,
    identity: Option<&str>,
) -> Result<()> {
    let Some(config) = &spec.sink.durable_outbox else {
        return Ok(());
    };
    config.validate()?;
    if !Path::new(&config.directory)
        .join("outbox.sqlite3")
        .try_exists()
        .map_err(io)?
    {
        if identity.is_some() {
            return Err(invalid(
                "registered outbox is missing; restore matching storage before retirement",
            ));
        }
        return Ok(());
    }
    let namespace = namespace.ok_or_else(|| invalid("outbox catalog identity is missing"))?;
    let out = Outbox::open(config, &binding(namespace, name, &spec.sink)?, false)?;
    if let Some(expected) = identity {
        if out.identity()? != expected {
            return Err(invalid("outbox UUID does not match the catalog"));
        }
    }
    let view = out.status()?;
    if view["pending_entries"] != 0 || view["dlq_entries"] != 0 || view["sender_active"] != false {
        return Err(invalid("retirement requires a paused, empty outbox with no active sender; resolve retained output first"));
    }
    Ok(())
}

/// Called by catalog publication BEFORE committing a new revision. A queue
/// cannot be removed, redirected or silently assigned a new retry policy.
pub(crate) fn check_update(
    old: &crate::spec::PipelineSpec,
    new: &crate::spec::PipelineSpec,
) -> Result<()> {
    if old.sink.durable_outbox != new.sink.durable_outbox
        || (old.sink.durable_outbox.is_some()
            && (old.sink.url != new.sink.url
                || old.sink.header_secret != new.sink.header_secret
                || old.sink.kind != new.sink.kind
                || old.sink.format != new.sink.format))
    {
        return Err(invalid("durable outbox binding is immutable for a pipeline; drain/resolve and retire it before choosing another target or policy"));
    }
    Ok(())
}

fn parse_id(uuid: &str, key: &str) -> Result<i64> {
    let id = key
        .strip_prefix(uuid)
        .and_then(|s| s.strip_prefix('-'))
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("entry id does not belong to this outbox"))?;
    Ok(id)
}
struct BoundQueue(Arc<Outbox>);
impl Drop for BoundQueue {
    fn drop(&mut self) {
        self.0.worker.store(false, Ordering::Release);
    }
}
impl DurableHttpQueue for BoundQueue {
    fn enqueue(&self, body: &[u8], now: u64) -> Result<String> {
        self.0.enqueue(body, now)
    }
    fn next(&self, now: u64, owner: &Arc<MemoryOwner>) -> Result<Option<DurableRequest>> {
        self.0.next(now, owner)
    }
    fn settle(&self, id: &str, attempt: u32, outcome: DurableOutcome, now: u64) -> Result<()> {
        self.0.settle(id, attempt, outcome, now)
    }
}
impl DurableHttpQueue for Outbox {
    fn enqueue(&self, body: &[u8], now: u64) -> Result<String> {
        Outbox::enqueue(self, body, now)
    }
    fn next(&self, now: u64, owner: &Arc<MemoryOwner>) -> Result<Option<DurableRequest>> {
        Outbox::next(self, now, owner)
    }
    fn settle(&self, id: &str, attempt: u32, outcome: DurableOutcome, now: u64) -> Result<()> {
        Outbox::settle(self, id, attempt, outcome, now)
    }
}

#[cfg(test)]
#[path = "outbox_tests.rs"]
mod tests;

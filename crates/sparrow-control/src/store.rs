//! SQLite catalog. A committed desired-state change does **not** wait for
//! MQTT/HTTP to come up — the supervisor converges afterwards.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use sparrow_model::{ErrorCode, Result, SparrowError};

use crate::reference_table::{
    reference_table_sha256, ReferenceTableMetadata, ReferenceTableRow, ReferenceTableSpec,
    MAX_REFERENCE_TABLE_BYTES_PER_NAME, MAX_REFERENCE_TABLE_CATALOG_BYTES,
    MAX_REFERENCE_TABLE_VERSIONS, MAX_REFERENCE_TABLE_NAMES,
    MAX_REFERENCE_TABLE_PREVIEW_PINS,
    REFERENCE_TABLE_METADATA_BYTES,
};
use crate::spec::PipelineSpec;
use crate::status::PipelineStatus;

pub const CATALOG_SCHEMA_VERSION: u32 = 4;
#[path="store_plugins.rs"]
mod plugin_catalog;
#[path = "store_reference_mutations.rs"]
mod reference_mutations;
pub const FORMAT_VERSION: u32 = 1;
const AUDIT_CAP: usize = 200;
const ATTEMPT_CAP: usize = 100;
static CATALOG_WORK: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(128);

/// Catalog handle. SQLite lives behind a mutex and is executed on the
/// bounded blocking pool (`run_blocking`) from async paths (R26).
#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    plugins: std::sync::OnceLock<Arc<sparrow_expr::plugins::Manager>>,
    conn: Mutex<Connection>,
    fail_before_commit: AtomicBool,
    stream_epoch: AtomicU64,
    reference_epoch: AtomicU64,
    status_effective: Mutex<StatusEffectiveCache>,
}

const STATUS_EFFECTIVE_CACHE_CAP: usize = 64;
const STATUS_EFFECTIVE_VALUE_CAP: usize = 4096;
#[derive(Default)]
struct StatusEffectiveCache {
    entries: VecDeque<StatusEffectiveEntry>,
    #[cfg(test)]
    misses: usize,
}
struct StatusEffectiveEntry {
    name: String,
    // SQLite data_version detects writes through OTHER Store/connections;
    // stream_epoch handles this connection's own schema writes.
    key: (u64, u64, u64, u64, u64),
    value: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct StreamRow {
    pub name: String,
    pub schema_json: String,
}

#[derive(Clone, Debug)]
pub struct PipelineRow {
    pub name: String,
    pub latest_revision: u64,
    pub spec: PipelineSpec,
    pub etag: String,
}

#[derive(Clone, Debug)]
pub struct DesiredState {
    pub name: String,
    pub revision: Option<u64>,
    /// Typed at the Rust boundary. SQLite column remains TEXT.
    pub status: PipelineStatus,
}

#[derive(Clone, Debug)]
pub struct ActualState {
    pub name: String,
    pub revision: Option<u64>,
    /// Typed at the Rust boundary. SQLite column remains TEXT.
    pub status: PipelineStatus,
    pub attempt_id: u64,
    pub consecutive_failures: u64,
    /// Durable safe-mode latch, independent of the bounded attempt history.
    pub restart_blocked: bool,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AttemptRow {
    pub id: i64,
    pub pipeline: String,
    pub revision: u64,
    pub outcome: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AuditRow {
    pub id: i64,
    pub at_ms: i64,
    pub actor: String,
    pub action: String,
    pub target: Option<String>,
    pub detail: Option<String>,
    pub outcome: String,
}

impl Store {
    pub fn configure_plugins(&self,manager:Arc<sparrow_expr::plugins::Manager>)->Result<()> {
        self.inner.plugins.set(manager).map_err(|_|SparrowError::new(ErrorCode::InvalidArgument,"plugin registry is already configured"))
    }
    pub fn plugins(&self)->Option<Arc<sparrow_expr::plugins::Manager>>{self.inner.plugins.get().cloned()}
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let conn = Connection::open(path).map_err(db)?;
        init(&conn)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                let mut perms = meta.permissions();
                perms.set_mode(0o600);
                let _ = std::fs::set_permissions(path, perms);
            }
        }
        note_secrets_key_on_open()?;
        Ok(Self {
            inner: Arc::new(StoreInner {
                plugins: std::sync::OnceLock::new(),
                conn: Mutex::new(conn),
                fail_before_commit: AtomicBool::new(false),
                stream_epoch: AtomicU64::new(0),
                reference_epoch: AtomicU64::new(0),
                status_effective: Mutex::new(StatusEffectiveCache::default()),
            }),
        })
    }

    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(db)?;
        init(&conn)?;
        note_secrets_key_on_open()?;
        Ok(Self {
            inner: Arc::new(StoreInner {
                plugins: std::sync::OnceLock::new(),
                conn: Mutex::new(conn),
                fail_before_commit: AtomicBool::new(false),
                stream_epoch: AtomicU64::new(0),
                reference_epoch: AtomicU64::new(0),
                status_effective: Mutex::new(StatusEffectiveCache::default()),
            }),
        })
    }

    pub fn schema_version(&self) -> Result<u32> {
        self.read(|c| meta_u32(c, "catalog_schema_version"))
    }

    pub fn put_stream(&self, name: &str, schema_json: &str) -> Result<()> {
        check_name(name)?;
        self.write(|c| {
            c.execute(
                "INSERT INTO streams(name, schema_json, created_at)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(name) DO UPDATE SET schema_json=excluded.schema_json",
                params![name, schema_json, now_ms()],
            )
            .map_err(db)?;
            // Under the connection/transaction lock. Failed commits may cause
            // an extra cache miss, never a cached answer for the wrong schema.
            self.inner.stream_epoch.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }

    pub fn get_stream(&self, name: &str) -> Result<StreamRow> {
        self.read(|c| {
            c.query_row(
                "SELECT name, schema_json FROM streams WHERE name=?1",
                [name],
                |r| {
                    Ok(StreamRow {
                        name: r.get(0)?,
                        schema_json: r.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(db)?
            .ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown stream `{name}`"),
                )
            })
        })
    }

    pub fn list_streams(&self) -> Result<Vec<StreamRow>> {
        self.read(load_streams)
    }

    /// Publish one immutable reference-table revision and atomically advance
    /// its latest head.  A revision is never updated in place: a failed CAS,
    /// validation, quota check, or commit leaves the old head untouched.
    pub fn publish_reference_table(
        &self,
        name: &str,
        expected_revision: u64,
        table: &ReferenceTableSpec,
    ) -> Result<ReferenceTableRow> {
        check_name(name)?;
        if expected_revision > i64::MAX as u64 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "reference table expected_revision exceeds SQLite integer range",
            ));
        }
        let payload = table.encoded_bytes()?;
        self.write(|c| {
            self.publish_reference_table_locked(c, name, expected_revision, table, &payload)
        })
    }

    /// The caller already holds Store::write's SQLite transaction.  Sharing
    /// this path makes full publication, mutations and rollback enforce the
    /// same CAS, quota, canonical digest and head integrity contract.
    fn publish_reference_table_locked(
        &self,
        c: &Connection,
        name: &str,
        expected_revision: u64,
        table: &ReferenceTableSpec,
        payload: &[u8],
    ) -> Result<ReferenceTableRow> {
        let payload_bytes = payload.len() as u64;
        let current: Option<i64> = c
            .query_row(
                "SELECT latest_revision FROM reference_table_heads WHERE name=?1",
                [name],
                |r| r.get(0),
            )
            .optional()
            .map_err(db)?;
        let current_u64 = match current {
            None => 0,
            Some(value) if value > 0 => value as u64,
            Some(_) => {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "reference table head revision is invalid",
                ));
            }
        };
        if current_u64 != 0 {
            // A head is an immutable revision pointer, not merely a CAS
            // counter.  Refuse to publish over a dangling/corrupt head.
            load_reference_table_metadata(c, name, current_u64)?;
        }
        if current_u64 != expected_revision {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "reference table `{name}` expected revision {expected_revision}, current {current_u64}"
                ),
            )
            .context("expected_revision", expected_revision.to_string())
            .context("current_revision", current_u64.to_string()));
        }
        let revision = expected_revision.checked_add(1).ok_or_else(|| {
            SparrowError::new(
                ErrorCode::BoundExceeded,
                "reference table revision exhausted",
            )
        })?;
        if revision > i64::MAX as u64 {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "reference table revision exceeds SQLite integer range",
            ));
        }
        let versions: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM reference_table_revisions WHERE name=?1",
                [name],
                |r| r.get(0),
            )
            .map_err(db)?;
        if versions as usize >= MAX_REFERENCE_TABLE_VERSIONS {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table `{name}` exceeds {MAX_REFERENCE_TABLE_VERSIONS} revisions"
                ),
            ));
        }
        if current.is_none() {
            let names: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM reference_table_heads",
                    [],
                    |r| r.get(0),
                )
                .map_err(db)?;
            if names as usize >= MAX_REFERENCE_TABLE_NAMES {
                return Err(SparrowError::new(
                    ErrorCode::BoundExceeded,
                    format!(
                        "reference table catalog exceeds {MAX_REFERENCE_TABLE_NAMES} names"
                    ),
                ));
            }
        }
        let table_bytes: i64 = c
            .query_row(
                "SELECT COALESCE(SUM(payload_bytes + ?2),0) FROM reference_table_revisions WHERE name=?1",
                params![name, REFERENCE_TABLE_METADATA_BYTES as i64],
                |r| r.get(0),
            )
            .map_err(db)?;
        if table_bytes < 0 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "reference table byte metadata is invalid",
            ));
        }
        if (table_bytes.max(0) as u64)
            .saturating_add(payload_bytes)
            .saturating_add(REFERENCE_TABLE_METADATA_BYTES)
            > MAX_REFERENCE_TABLE_BYTES_PER_NAME
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table `{name}` exceeds {MAX_REFERENCE_TABLE_BYTES_PER_NAME}B across revisions"
                ),
            ));
        }
        let catalog_bytes: i64 = c
            .query_row(
                "SELECT COALESCE(SUM(payload_bytes + ?1),0) FROM reference_table_revisions",
                [REFERENCE_TABLE_METADATA_BYTES as i64],
                |r| r.get(0),
            )
            .map_err(db)?;
        if catalog_bytes < 0 {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "reference table catalog byte metadata is invalid",
            ));
        }
        if (catalog_bytes.max(0) as u64)
            .saturating_add(payload_bytes)
            .saturating_add(REFERENCE_TABLE_METADATA_BYTES)
            > MAX_REFERENCE_TABLE_CATALOG_BYTES
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "reference table catalog exceeds {MAX_REFERENCE_TABLE_CATALOG_BYTES}B"
                ),
            ));
        }
        let sha256 = reference_table_sha256(name, revision, table)?;
        let table_json = String::from_utf8(payload.to_vec()).map_err(|_| {
            SparrowError::new(
                ErrorCode::InvalidSchema,
                "reference table JSON is not UTF-8",
            )
        })?;
        let created_at_ms = now_ms();
        c.execute(
            "INSERT INTO reference_table_revisions
                (name, revision, table_json, sha256, payload_bytes, row_count, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                name,
                revision as i64,
                table_json,
                sha256,
                payload_bytes as i64,
                table.rows.len() as i64,
                created_at_ms,
            ],
        )
        .map_err(db)?;
        if current.is_some() {
            c.execute(
                "UPDATE reference_table_heads SET latest_revision=?1 WHERE name=?2",
                params![revision as i64, name],
            )
            .map_err(db)?;
        } else {
            c.execute(
                "INSERT INTO reference_table_heads(name, latest_revision) VALUES (?1, ?2)",
                params![name, revision as i64],
            )
            .map_err(db)?;
        }
        // A failed transaction may cause an extra status-cache miss, but
        // must never leave a stale successful answer after a commit.
        self.inner.reference_epoch.fetch_add(1, Ordering::Relaxed);
        Ok(ReferenceTableRow {
            name: name.to_string(),
            revision,
            sha256,
            table: table.clone(),
            payload_bytes,
            row_count: table.rows.len() as u64,
            created_at_ms,
        })
    }

    /// Load one exact immutable revision.  Stored JSON and its digest are
    /// revalidated on every read so a corrupt catalog fails closed.
    pub fn get_reference_table_revision(
        &self,
        name: &str,
        revision: u64,
    ) -> Result<ReferenceTableRow> {
        check_name(name)?;
        if revision == 0 || revision > i64::MAX as u64 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "reference table revision must be 1..=i64::MAX",
            ));
        }
        self.read(|c| load_reference_table_revision(c, name, revision))
    }

    /// Load the current head only.  Pipeline bindings must use
    /// `get_reference_table_revision`, never this moving alias.
    pub fn get_reference_table(&self, name: &str) -> Result<ReferenceTableRow> {
        check_name(name)?;
        self.read(|c| {
            let revision: i64 = c
                .query_row(
                    "SELECT latest_revision FROM reference_table_heads WHERE name=?1",
                    [name],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db)?
                .ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("unknown reference table `{name}`"),
                    )
                })?;
            if revision <= 0 {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "reference table head revision is invalid",
                ));
            }
            load_reference_table_revision(c, name, revision as u64)
        })
    }

    /// List current heads, ordered by table name.  Historical revisions are
    /// intentionally omitted; use `list_reference_table_revisions` for them.
    pub fn list_reference_tables(&self) -> Result<Vec<ReferenceTableMetadata>> {
        self.read(|c| {
            let mut stmt = c
                .prepare(
                    "SELECT name, latest_revision FROM reference_table_heads ORDER BY name",
                )
                .map_err(db)?;
            let heads = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
                .map_err(db)?;
            let mut out = Vec::new();
            for head in heads {
                let (name, revision) = head.map_err(db)?;
                if revision <= 0 {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "reference table head revision is invalid",
                    ));
                }
                out.push(load_reference_table_metadata(c, &name, revision as u64)?);
            }
            Ok(out)
        })
    }

    /// List every immutable revision of one table, ordered by revision.
    pub fn list_reference_table_revisions(
        &self,
        name: &str,
    ) -> Result<Vec<ReferenceTableMetadata>> {
        check_name(name)?;
        self.read(|c| {
            let mut stmt = c
                .prepare(
                    "SELECT revision FROM reference_table_revisions
                     WHERE name=?1 ORDER BY revision",
                )
                .map_err(db)?;
            let revisions = stmt
                .query_map([name], |r| r.get::<_, i64>(0))
                .map_err(db)?;
            let mut out = Vec::new();
            for revision in revisions {
                out.push(load_reference_table_metadata(
                    c,
                    name,
                    revision.map_err(db)? as u64,
                )?);
            }
            Ok(out)
        })
    }

    /// Resolve and digest-check every table explicitly bound by a pipeline.
    /// This read API is for validation/startup; `put_pipeline` repeats the
    /// same check inside its own write transaction to close the TOCTOU gap.
    pub fn reference_bindings(&self, spec: &PipelineSpec) -> Result<Vec<ReferenceTableRow>> {
        if spec.reference_tables.is_empty() {
            return Ok(Vec::new());
        }
        self.read(|c| validate_reference_bindings(c, spec))
    }

    /// Read-only dependency preview for operations/diagnostics.  All queries
    /// run inside one SQLite read transaction so the latest head, revision
    /// metadata and persistent pipeline pins describe one catalog snapshot.
    /// Pin rows are intentionally capped for response size; `total_count` and
    /// `truncated` make that loss explicit.  GC never consumes this preview
    /// and always evaluates the complete dependency relation in its write
    /// transaction.
    pub fn reference_table_dependencies(&self, name: &str) -> Result<serde_json::Value> {
        check_name(name)?;
        let guard = self
            .inner
            .conn
            .lock()
            .map_err(|_| SparrowError::new(ErrorCode::Internal, "catalog mutex poisoned"))?;
        guard.execute_batch("BEGIN DEFERRED").map_err(db)?;
        let result = (|| {
            let latest: Option<i64> = guard
                .query_row(
                    "SELECT latest_revision FROM reference_table_heads WHERE name=?1",
                    [name],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db)?;
            if latest.is_some_and(|revision| revision <= 0) {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "reference table head revision is invalid",
                ));
            }
            if let Some(revision) = latest {
                // Keep the preview fail-closed for a dangling head instead
                // of presenting a plausible dependency report for a table
                // that a job could not actually bind.
                load_reference_table_metadata(&guard, name, revision as u64)?;
            }

            let mut revisions = Vec::new();
            let mut revision_stmt = guard
                .prepare(
                    "SELECT revision FROM reference_table_revisions
                     WHERE name=?1 ORDER BY revision",
                )
                .map_err(db)?;
            let revision_rows = revision_stmt
                .query_map([name], |r| r.get::<_, i64>(0))
                .map_err(db)?;
            for revision in revision_rows {
                let revision = revision.map_err(db)?;
                if revision <= 0 || revision as u64 > i64::MAX as u64 {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "reference table revision metadata is invalid",
                    ));
                }
                revisions.push(load_reference_table_metadata(
                    &guard,
                    name,
                    revision as u64,
                )?);
                if revisions.len() > MAX_REFERENCE_TABLE_VERSIONS {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        format!(
                            "reference table `{name}` exceeds {MAX_REFERENCE_TABLE_VERSIONS} revisions"
                        ),
                    ));
                }
            }
            drop(revision_stmt);

            let total_pins: i64 = guard
                .query_row(
                    "SELECT COUNT(*) FROM pipeline_reference_tables WHERE table_name=?1",
                    [name],
                    |r| r.get(0),
                )
                .map_err(db)?;
            if total_pins < 0 {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "reference table pin count is invalid",
                ));
            }
            let mut pin_stmt = guard
                .prepare(
                    "SELECT pipeline_name, pipeline_revision, table_revision, sha256
                     FROM pipeline_reference_tables
                     WHERE table_name=?1
                     ORDER BY table_revision, pipeline_name, pipeline_revision
                     LIMIT ?2",
                )
                .map_err(db)?;
            let pin_rows = pin_stmt
                .query_map(
                    params![name, MAX_REFERENCE_TABLE_PREVIEW_PINS as i64],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, i64>(2)?,
                            r.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(db)?;
            let mut pins = Vec::new();
            for pin in pin_rows {
                let (pipeline, pipeline_revision, table_revision, sha256) = pin.map_err(db)?;
                if pipeline_revision <= 0 || table_revision <= 0 || !valid_sha256(&sha256) {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "reference table dependency pin metadata is invalid",
                    ));
                }
                pins.push(serde_json::json!({
                    "pipeline": pipeline,
                    "pipeline_revision": pipeline_revision,
                    "table_revision": table_revision,
                    "sha256": sha256,
                }));
            }
            drop(pin_stmt);
            let total_pins = total_pins as u64;
            let revision_count = revisions.len();
            let returned_pin_count = pins.len();
            Ok(serde_json::json!({
                "name": name,
                "latest_revision": latest.map(|revision| revision as u64),
                "revisions": revisions,
                "revision_count": revision_count,
                "pins": {
                    "items": pins,
                    "returned_count": returned_pin_count,
                    "total_count": total_pins,
                    "max_returned": MAX_REFERENCE_TABLE_PREVIEW_PINS,
                    "truncated": total_pins > MAX_REFERENCE_TABLE_PREVIEW_PINS as u64,
                    "kind": "persistent_pipeline_revision_dependency"
                },
                "gc": {
                    "uses_complete_pin_relation": true,
                    "preview_truncation_does_not_change_gc_roots": true
                }
            }))
        })();
        match result {
            Ok(value) => {
                match guard.execute_batch("COMMIT") {
                    Ok(()) => Ok(value),
                    Err(error) => {
                        let _ = guard.execute_batch("ROLLBACK");
                        Err(db(error))
                    }
                }
            }
            Err(error) => {
                let _ = guard.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Conservative B1 GC for one table.  No active-job/checkpoint deletion
    /// is claimed here: every persisted pipeline revision is a permanent
    /// dependency until a future explicit pipeline-retention API exists.
    pub fn gc_reference_table(&self, name: &str) -> Result<serde_json::Value> {
        check_name(name)?;
        self.write(|c| {
            let latest: Option<i64> = c
                .query_row(
                    "SELECT latest_revision FROM reference_table_heads WHERE name=?1",
                    [name],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db)?;
            let Some(latest) = latest else {
                return Ok(serde_json::json!({
                    "name": name,
                    "deleted": 0,
                    "pinned": false,
                    "pinned_revisions": [],
                }));
            };
            if latest <= 0 {
                return Err(SparrowError::new(
                    ErrorCode::CodecViolation,
                    "reference table head revision is invalid",
                ));
            }
            // Do not let GC operate on a dangling/corrupt head.  Otherwise a
            // damaged catalog could advance deletion while its advertised
            // latest revision is already unavailable to a future job.
            let _latest_metadata =
                load_reference_table_metadata(c, name, latest as u64)?;
            let mut pinned_revisions = Vec::new();
            {
                let mut stmt = c
                    .prepare(
                    "SELECT DISTINCT table_revision, sha256
                     FROM pipeline_reference_tables
                     WHERE table_name=?1
                     ORDER BY table_revision",
                )
                .map_err(db)?;
                let rows = stmt
                    .query_map(params![name], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                    })
                    .map_err(db)?;
                for revision in rows {
                    let (revision, sha256) = revision.map_err(db)?;
                    if revision <= 0 {
                        return Err(SparrowError::new(
                            ErrorCode::CodecViolation,
                            "reference table pin revision is invalid",
                        ));
                    }
                    if !valid_sha256(&sha256) {
                        return Err(SparrowError::new(
                            ErrorCode::CodecViolation,
                            "reference table pin digest is invalid",
                        ));
                    }
                    if revision != latest {
                        pinned_revisions.push(revision as u64);
                    }
                }
            }
            c.execute(
                "DELETE FROM reference_table_revisions
                 WHERE name=?1 AND revision != ?2
                   AND NOT EXISTS (
                       SELECT 1 FROM pipeline_reference_tables p
                       WHERE p.table_name=reference_table_revisions.name
                         AND p.table_revision=reference_table_revisions.revision
                   )",
                params![name, latest],
            )
            .map_err(db)?;
            let deleted = c.changes();
            if deleted > 0 {
                self.inner.reference_epoch.fetch_add(1, Ordering::Relaxed);
            }
            Ok(serde_json::json!({
                "name": name,
                "deleted": deleted,
                "pinned": !pinned_revisions.is_empty(),
                "pinned_revisions": pinned_revisions,
                "latest_revision": latest as u64,
            }))
        })
    }

    pub fn put_pipeline(
        &self,
        name: &str,
        spec: &PipelineSpec,
        expected_etag: Option<&str>,
    ) -> Result<PipelineRow> {
        self.put_pipeline_with_activation(name, spec, expected_etag, false)
    }

    /// Restore publishes its configuration and desired revision atomically.
    /// There is no intermediate "stop old, but accidentally restart fresh"
    /// state if a request is cancelled or the process exits between steps.
    pub fn put_pipeline_and_start(
        &self,
        name: &str,
        spec: &PipelineSpec,
        expected_etag: Option<&str>,
    ) -> Result<PipelineRow> {
        self.put_pipeline_with_activation(name, spec, expected_etag, true)
    }

    fn put_pipeline_with_activation(
        &self,
        name: &str,
        spec: &PipelineSpec,
        expected_etag: Option<&str>,
        activate: bool,
    ) -> Result<PipelineRow> {
        check_name(name)?;
        let plugin_refs=plugin_catalog::references(spec)?;
        self.write(|c| {
            let current: Option<(u64, String)> = c
                .query_row(
                    "SELECT latest_revision, etag FROM pipelines WHERE name=?1",
                    [name],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(db)?;
            if let Some((_, etag)) = &current {
                match expected_etag {
                    None => {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "If-Match is required to update an existing pipeline",
                        )
                        .context("hint", "If-Match: rev-N"));
                    }
                    Some(want) if want != etag && want != &*format!("\"{etag}\"") => {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            format!("If-Match `{want}` does not match current `{etag}`"),
                        )
                        .context("etag", etag.clone()));
                    }
                    Some(_) => {}
                }
            }
            let next = current.map(|(r, _)| r + 1).unwrap_or(1);
            let etag = format!("rev-{next}");
            let spec_json = serde_json::to_string(spec).map_err(|e| {
                SparrowError::new(ErrorCode::InvalidArgument, format!("encode spec: {e}"))
            })?;
            let ts = now_ms();
            c.execute(
                "INSERT INTO pipelines(name, latest_revision, etag, created_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(name) DO UPDATE SET latest_revision=excluded.latest_revision, etag=excluded.etag",
                params![name, next as i64, etag, ts],
            )
            .map_err(db)?;
            c.execute(
                "INSERT INTO pipeline_revisions(name, revision, spec_json, etag, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![name, next as i64, spec_json, etag, ts],
            )
            .map_err(db)?;
            if !plugin_refs.is_empty() {
                crate::plugins::manager(self)?.with_references(&plugin_refs,||plugin_catalog::insert(c,name,next,&plugin_refs))?;
            }
            // Materialize every immutable table dependency in the same
            // transaction as the pipeline revision.  GC therefore cannot
            // observe a committed pipeline whose referenced revision is not
            // yet pinned.  The helper also rechecks the digest, closing the
            // validation-then-publish TOCTOU window.
            for table in validate_reference_bindings(c, spec)? {
                c.execute(
                    "INSERT INTO pipeline_reference_tables
                        (pipeline_name, pipeline_revision, table_name, table_revision, sha256)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        name,
                        next as i64,
                        table.name,
                        table.revision as i64,
                        table.sha256,
                    ],
                )
                .map_err(db)?;
            }
            c.execute(
                "INSERT OR IGNORE INTO desired_state(name, desired_revision, desired_status, updated_at)
                 VALUES (?1, NULL, 'stopped', ?2)",
                params![name, ts],
            )
            .map_err(db)?;
            c.execute(
                "INSERT OR IGNORE INTO actual_state(name, actual_revision, actual_status, attempt_id, consecutive_failures, last_error, updated_at)
                 VALUES (?1, NULL, 'stopped', 0, 0, NULL, ?2)",
                params![name, ts],
            )
            .map_err(db)?;
            if activate {Self::start_revision(c,name,next)?;}
            Ok(PipelineRow {
                name: name.to_string(),
                latest_revision: next,
                spec: spec.clone(),
                etag,
            })
        })
    }

    pub fn get_pipeline(&self, name: &str) -> Result<PipelineRow> {
        self.read(|c| load_pipeline(c, name))
    }

    /// Latest stored spec and its effective guarantees, not the running plan.
    /// Binding stays outside the catalog mutex; only small, bounded results
    /// are cached. Failed binding remains a readable status with unknown eligibility.
    pub fn effective_pipeline_status(
        &self,
        name: &str,
    ) -> Result<(PipelineRow, serde_json::Value)> {
        let (row, key, cached) = self.read(|c| {
            let version: u64 = c
                .query_row("PRAGMA data_version", [], |r| r.get(0))
                .map_err(db)?;
            let row = load_pipeline(c, name)?;
            let key = (
                row.latest_revision,
                self.inner.stream_epoch.load(Ordering::Relaxed),
                self.inner.reference_epoch.load(Ordering::Relaxed),
                self.plugins().map_or(0,|p|p.epoch().saturating_add(1)),
                version,
            );
            let mut cache = self
                .inner
                .status_effective
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(i) = cache
                .entries
                .iter()
                .position(|e| e.name == name && e.key == key)
            {
                let entry = cache.entries.remove(i).expect("cache index");
                let value = entry.value.clone();
                cache.entries.push_back(entry);
                return Ok((row, key, Some(value)));
            }
            drop(cache);
            Ok((row, key, None))
        })?;
        if let Some(value) = cached {
            return Ok((row, value));
        }
        // Binding is intentionally outside the SQLite mutex.  The wrapper
        // resolves immutable table revisions and their digests, then performs
        // the same managed-table checks as validate/startup.
        let plan = crate::validate::bind_plan_with_store(
            self,
            &row.spec,
            name,
            row.latest_revision,
        );
        let effective = match plan {
            Ok(plan) => crate::validate::effective_guarantees_with_plan(&row.spec, &plan),
            Err(e) => {
                let mut value = crate::validate::effective_guarantees(&row.spec);
                value["aligned_eligible"] = serde_json::Value::Null;
                value["aligned_eligibility_reason"] =
                    serde_json::json!(format!("plan_bind_failed: {}", e.message));
                value
            }
        };
        let mut cache = self
            .inner
            .status_effective
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        #[cfg(test)]
        {
            cache.misses += 1;
        }
        cache.entries.retain(|e| e.name != name);
        if serde_json::to_vec(&effective).is_ok_and(|v| v.len() <= STATUS_EFFECTIVE_VALUE_CAP) {
            if cache.entries.len() == STATUS_EFFECTIVE_CACHE_CAP {
                cache.entries.pop_front();
            }
            cache.entries.push_back(StatusEffectiveEntry {
                name: name.into(),
                key,
                value: effective.clone(),
            });
        }
        Ok((row, effective))
    }

    /// Load the spec for a specific revision (R23). Does not rewrite latest.
    pub fn get_pipeline_revision(&self, name: &str, revision: u64) -> Result<PipelineRow> {
        self.read(|c| load_pipeline_revision(c, name, revision))
    }

    pub fn list_pipeline_names(&self) -> Result<Vec<String>> {
        self.read(|c| {
            let mut stmt = c
                .prepare("SELECT name FROM pipelines ORDER BY name")
                .map_err(db)?;
            let rows = stmt.query_map([], |r| r.get(0)).map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    pub fn set_desired(&self, name: &str, status: &str, revision: Option<u64>) -> Result<()> {
        let status = PipelineStatus::parse(status)?;
        if !status.is_desired() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "desired status must be running or stopped, got '{}'",
                    status.as_str()
                ),
            ));
        }
        self.write(|c| {
            let exists: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM pipelines WHERE name=?1",
                    [name],
                    |r| r.get(0),
                )
                .map_err(db)?;
            if exists == 0 {
                return Err(SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("unknown pipeline `{name}`"),
                ));
            }
            c.execute(
                "INSERT INTO desired_state(name, desired_revision, desired_status, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(name) DO UPDATE SET
                    desired_revision=excluded.desired_revision,
                    desired_status=excluded.desired_status,
                    updated_at=excluded.updated_at",
                params![name, revision.map(|r| r as i64), status.as_str(), now_ms()],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub fn set_actual(
        &self,
        name: &str,
        status: &str,
        revision: Option<u64>,
        attempt_id: u64,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.set_actual_inner(name, status, revision, attempt_id, last_error, true)
    }

    /// Capacity/temporary admission failures wait without consuming the crash cap.
    pub fn set_waiting_failure(
        &self,
        name: &str,
        revision: u64,
        attempt: u64,
        error: &str,
    ) -> Result<()> {
        self.set_actual_inner(name, "waiting", Some(revision), attempt, Some(error), false)
    }

    fn set_actual_inner(
        &self,
        name: &str,
        status: &str,
        revision: Option<u64>,
        attempt_id: u64,
        last_error: Option<&str>,
        count_failure: bool,
    ) -> Result<()> {
        let status = PipelineStatus::parse(status)?;
        self.write(|c| {
            let prev: i64 = c
                .query_row(
                    "SELECT consecutive_failures FROM actual_state WHERE name=?1",
                    [name],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db)?
                .unwrap_or(0);
            let consecutive = match status {
                PipelineStatus::Completed => 0,
                PipelineStatus::Failed if count_failure => prev.saturating_add(1),
                _ => prev,
            };
            c.execute(
                "INSERT INTO actual_state(name, actual_revision, actual_status, attempt_id, consecutive_failures, last_error, updated_at, restart_blocked)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(name) DO UPDATE SET
                    actual_revision=excluded.actual_revision,
                    actual_status=excluded.actual_status,
                    attempt_id=excluded.attempt_id,
                    consecutive_failures=excluded.consecutive_failures,
                    restart_blocked=CASE
                        WHEN excluded.actual_status='failed' THEN 1
                        WHEN excluded.actual_status IN ('running', 'completed') THEN 0
                        ELSE actual_state.restart_blocked END,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at",
                params![
                    name,
                    revision.map(|r| r as i64),
                    status.as_str(),
                    attempt_id as i64,
                    consecutive,
                    last_error,
                    now_ms(),
                    status == PipelineStatus::Failed
                ],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    /// Update `last_error` without changing status, attempt_id, or consecutive_failures.
    pub fn set_last_error(&self, name: &str, last_error: Option<&str>) -> Result<()> {
        self.write(|c| {
            c.execute(
                "UPDATE actual_state SET last_error=?1, updated_at=?2 WHERE name=?3",
                params![last_error, now_ms(), name],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    /// Reset crash counting after a stable run. Does not unlock safe-mode.
    pub fn reset_consecutive_failures(&self, name: &str) -> Result<()> {
        self.write(|c| {
            c.execute(
                "UPDATE actual_state SET consecutive_failures=0, updated_at=?1 WHERE name=?2",
                params![now_ms(), name],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    /// An explicit start atomically commits desired state and clears both holds.
    /// Starting the same running revision is idempotent; all other states/revisions
    /// reset to stopped so converge can start (or retry) the requested revision.
    /// Merely pruning history, stopping, or restarting the process cannot unlock.
    pub(crate) fn request_start_revision(&self, name: &str, revision: u64) -> Result<()> {
        self.write(|c| Self::start_revision(c, name, revision))
    }

    fn start_revision(c: &Connection, name: &str, revision: u64) -> Result<()> {
        let changed = c
            .execute(
                "UPDATE desired_state SET desired_status='running', desired_revision=?1,
                    updated_at=?2 WHERE name=?3",
                params![revision as i64, now_ms(), name],
            )
            .map_err(db)?;
        let actual = c
            .execute(
                "UPDATE actual_state SET
                    actual_status=CASE WHEN actual_status='running' AND actual_revision=?3
                        THEN 'running' ELSE 'stopped' END,
                    actual_revision=CASE WHEN actual_status='running' AND actual_revision=?3
                        THEN actual_revision ELSE NULL END,
                    consecutive_failures=0, restart_blocked=0, last_error=NULL,
                    updated_at=?1 WHERE name=?2",
                params![now_ms(), name, revision as i64],
            )
            .map_err(db)?;
        if changed != 1 || actual != 1 {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "missing pipeline start state",
            ));
        }
        Ok(())
    }

    pub fn desired(&self, name: &str) -> Result<DesiredState> {
        self.read(|c| {
            let (name, rev, status): (String, Option<i64>, String) = c
                .query_row(
                    "SELECT name, desired_revision, desired_status FROM desired_state WHERE name=?1",
                    [name],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .map_err(db)?
                .ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::InvalidArgument,
                        format!("no desired state for `{name}`"),
                    )
                })?;
            Ok(DesiredState {
                name,
                revision: rev.map(|v| v as u64),
                status: PipelineStatus::parse(&status)?,
            })
        })
    }

    pub fn actual(&self, name: &str) -> Result<ActualState> {
        self.read(|c| {
            let (name, rev, status, attempt, failures, last_error, restart_blocked): (
                String,
                Option<i64>,
                String,
                i64,
                i64,
                Option<String>,
                bool,
            ) = c
                .query_row(
                    "SELECT name, actual_revision, actual_status, attempt_id, COALESCE(consecutive_failures, 0), last_error, restart_blocked FROM actual_state WHERE name=?1",
                    [name],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
                )
                .optional()
                .map_err(db)?
                .ok_or_else(|| {
                    SparrowError::new(ErrorCode::InvalidArgument, format!("no actual state for `{name}`"))
                })?;
            Ok(ActualState {
                name,
                revision: rev.map(|v| v as u64),
                status: PipelineStatus::parse(&status)?,
                attempt_id: attempt as u64,
                consecutive_failures: failures as u64,
                restart_blocked,
                last_error,
            })
        })
    }

    pub fn list_desired(&self) -> Result<Vec<DesiredState>> {
        self.read(|c| {
            let mut stmt = c
                .prepare("SELECT name, desired_revision, desired_status FROM desired_state")
                .map_err(db)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(db)?;
            let mut out = Vec::new();
            for row in rows {
                let (name, rev, status) = row.map_err(db)?;
                out.push(DesiredState {
                    name,
                    revision: rev.map(|v| v as u64),
                    status: PipelineStatus::parse(&status)?,
                });
            }
            Ok(out)
        })
    }

    /// Process restart: in-memory jobs are gone. This is `restart_fresh`, not restore.
    pub fn reset_actual_after_process_restart(&self) -> Result<()> {
        self.write(|c| {
            // Fixed-point replay requires a fresh explicit start after process
            // restart. Do not silently replay days of output from an old pin.
            let rows={
                let mut query=c.prepare("SELECT name, desired_revision FROM desired_state WHERE desired_status='running'").map_err(db)?;
                let rows=query.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<u64>>(1)?))).map_err(db)?;
                rows.collect::<std::result::Result<Vec<_>,_>>().map_err(db)?
            };
            for (name,revision) in rows {
                if let Some(revision)=revision {
                    // Invalid revisions fail per pipeline during convergence;
                    // they must not prevent healthy siblings from booting.
                    if load_pipeline_revision(c,&name,revision).is_ok_and(|r| r.spec.requires_explicit_restart()) {
                        c.execute("UPDATE actual_state SET restart_blocked=1,last_error=CASE WHEN last_error LIKE 'held:%' THEN last_error ELSE 'held: fixed snapshot or external plugin requires explicit start after process restart; last: ' || COALESCE(last_error,'unknown') END WHERE name=?1",[&name]).map_err(db)?;
                    }
                }
            }
            c.execute(
                "UPDATE actual_state SET actual_status='stopped', updated_at=?1",
                params![now_ms()],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub fn insert_attempt(
        &self,
        pipeline: &str,
        revision: u64,
        outcome: &str,
        detail: Option<&str>,
    ) -> Result<i64> {
        self.write(|c| {
            c.execute(
                "INSERT INTO deployment_attempts(pipeline, revision, started_at, outcome, detail, restore_claim)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'none')",
                params![pipeline, revision as i64, now_ms(), outcome, detail],
            )
            .map_err(db)?;
            let id = c.last_insert_rowid();
            c.execute(
                "DELETE FROM deployment_attempts WHERE id NOT IN (
                    SELECT id FROM deployment_attempts ORDER BY id DESC LIMIT ?1
                )",
                params![ATTEMPT_CAP as i64],
            )
            .map_err(db)?;
            Ok(id)
        })
    }

    pub fn last_attempt(&self, pipeline: &str) -> Result<Option<AttemptRow>> {
        self.read(|c| {
            c.query_row(
                "SELECT id, pipeline, revision, outcome, detail FROM deployment_attempts
                 WHERE pipeline=?1 ORDER BY id DESC LIMIT 1",
                [pipeline],
                |r| {
                    Ok(AttemptRow {
                        id: r.get(0)?,
                        pipeline: r.get(1)?,
                        revision: r.get::<_, i64>(2)? as u64,
                        outcome: r.get(3)?,
                        detail: r.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(db)
        })
    }

    pub fn audit(
        &self,
        actor: &str,
        action: &str,
        target: Option<&str>,
        detail: Option<&str>,
        outcome: &str,
    ) -> Result<()> {
        self.write(|c| {
            c.execute(
                "INSERT INTO audit_log(at_ms, actor, action, target, detail, outcome)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![now_ms(), actor, action, target, detail, outcome],
            )
            .map_err(db)?;
            c.execute(
                "DELETE FROM audit_log WHERE id NOT IN (
                    SELECT id FROM audit_log ORDER BY id DESC LIMIT ?1
                )",
                params![AUDIT_CAP as i64],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub fn list_audit(&self, limit: usize) -> Result<Vec<AuditRow>> {
        let limit = limit.min(AUDIT_CAP).max(1);
        self.read(|c| {
            let mut stmt = c
                .prepare(
                    "SELECT id, at_ms, actor, action, target, detail, outcome
                     FROM audit_log ORDER BY id DESC LIMIT ?1",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([limit as i64], |r| {
                    Ok(AuditRow {
                        id: r.get(0)?,
                        at_ms: r.get(1)?,
                        actor: r.get(2)?,
                        action: r.get(3)?,
                        target: r.get(4)?,
                        detail: r.get(5)?,
                        outcome: r.get(6)?,
                    })
                })
                .map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    pub fn put_secret(&self, name: &str, value: &str) -> Result<()> {
        check_name(name)?;
        if value.len() > 4096 {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                "secret value exceeds 4KiB",
            ));
        }
        let stored = seal_secret(value)?;
        self.write(|c| {
            c.execute(
                "INSERT INTO secrets(name, value) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                params![name, stored],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub fn get_secret(&self, name: &str) -> Result<Option<String>> {
        self.read(|c| {
            let raw: Option<String> = c
                .query_row("SELECT value FROM secrets WHERE name=?1", [name], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(db)?;
            match raw {
                None => Ok(None),
                Some(v) => unseal_secret(&v).map(Some),
            }
        })
    }

    pub fn put_allow(&self, host: &str, port: u16) -> Result<()> {
        self.write(|c| {
            c.execute(
                "INSERT OR IGNORE INTO allowlist(host, port) VALUES (?1, ?2)",
                params![host, port as i64],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub fn allowlist(&self) -> Result<Vec<(String, u16)>> {
        self.read(|c| {
            let mut stmt = c.prepare("SELECT host, port FROM allowlist").map_err(db)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get::<_, i64>(1)? as u16)))
                .map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let g = self
            .inner
            .conn
            .lock()
            .map_err(|_| SparrowError::new(ErrorCode::Internal, "catalog mutex poisoned"))?;
        f(&g)
    }

    fn write<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let g = self
            .inner
            .conn
            .lock()
            .map_err(|_| SparrowError::new(ErrorCode::Internal, "catalog mutex poisoned"))?;
        g.execute_batch("BEGIN IMMEDIATE").map_err(db)?;
        match f(&g) {
            Ok(v) => {
                if self.inner.fail_before_commit.swap(false, Ordering::SeqCst) {
                    let _ = g.execute_batch("ROLLBACK");
                    return Err(SparrowError::new(
                        ErrorCode::Internal,
                        "injected catalog crash before COMMIT",
                    ));
                }
                match g.execute_batch("COMMIT") {
                    Ok(()) => Ok(v),
                    Err(error) => {
                        // In rollback-journal mode a concurrent read snapshot
                        // may make COMMIT return SQLITE_BUSY while keeping this
                        // transaction open. Never expose its uncommitted head
                        // or poison the next BEGIN after reporting failure.
                        let _ = g.execute_batch("ROLLBACK");
                        Err(db(error).context("catalog_stage", "commit"))
                    }
                }
            }
            Err(e) => {
                let _ = g.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Run a catalog operation on Tokio's bounded blocking pool (R26).
    /// Async workers must use this instead of calling SQLite inline.
    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        let permit = CATALOG_WORK.try_acquire().map_err(|_| {
            SparrowError::new(
                ErrorCode::ResourceExhausted,
                "catalog/operation worker capacity exhausted",
            )
            .retryable(true)
        })?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f()
        })
        .await
        .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("catalog worker: {e}")))?
    }

    /// Next write runs to completion then rolls back (simulates crash mid-commit).
    pub fn debug_fail_next_commit(&self) {
        self.inner.fail_before_commit.store(true, Ordering::SeqCst);
    }

    /// Test helper: raw sealed secret bytes (never used in logs).
    pub fn debug_raw_secret(&self, name: &str) -> Result<String> {
        self.read(|c| {
            c.query_row("SELECT value FROM secrets WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .map_err(db)
        })
    }
}

fn load_pipeline_revision(c: &Connection, name: &str, revision: u64) -> Result<PipelineRow> {
    let (latest, etag): (i64, String) = c
        .query_row(
            "SELECT latest_revision, etag FROM pipelines WHERE name=?1",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(db)?
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("unknown pipeline `{name}`"),
            )
        })?;
    let spec_json: String = c
        .query_row(
            "SELECT spec_json FROM pipeline_revisions WHERE name=?1 AND revision=?2",
            params![name, revision as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(db)?
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("pipeline `{name}` has no revision {revision}"),
            )
        })?;
    let spec: PipelineSpec = serde_json::from_str(&spec_json)
        .map_err(|e| SparrowError::new(ErrorCode::InvalidSchema, format!("stored spec: {e}")))?;
    let etag = if revision == latest as u64 {
        etag
    } else {
        format!("rev-{revision}")
    };
    Ok(PipelineRow {
        name: name.to_string(),
        latest_revision: latest as u64,
        spec,
        etag,
    })
}

fn load_pipeline(c: &Connection, name: &str) -> Result<PipelineRow> {
    let (rev, etag): (i64, String) = c
        .query_row(
            "SELECT latest_revision, etag FROM pipelines WHERE name=?1",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(db)?
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("unknown pipeline `{name}`"),
            )
        })?;
    let spec_json: String = c
        .query_row(
            "SELECT spec_json FROM pipeline_revisions WHERE name=?1 AND revision=?2",
            params![name, rev],
            |r| r.get(0),
        )
        .map_err(db)?;
    let spec: PipelineSpec = serde_json::from_str(&spec_json)
        .map_err(|e| SparrowError::new(ErrorCode::InvalidSchema, format!("stored spec: {e}")))?;
    Ok(PipelineRow {
        name: name.to_string(),
        latest_revision: rev as u64,
        spec,
        etag,
    })
}

fn load_streams(c: &Connection) -> Result<Vec<StreamRow>> {
    let mut stmt = c
        .prepare("SELECT name, schema_json FROM streams ORDER BY name")
        .map_err(db)?;
    let rows = stmt
        .query_map([], |r| {
            Ok(StreamRow {
                name: r.get(0)?,
                schema_json: r.get(1)?,
            })
        })
        .map_err(db)?;
    rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
}

fn load_reference_table_revision(
    c: &Connection,
    name: &str,
    revision: u64,
) -> Result<ReferenceTableRow> {
    if revision == 0 || revision > i64::MAX as u64 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "reference table revision is outside the storage range",
        ));
    }
    let (stored_name, stored_revision, table_json, stored_sha, payload_bytes, row_count, created_at): (
        String,
        i64,
        Option<String>,
        String,
        i64,
        i64,
        i64,
    ) = c
        .query_row(
            "SELECT name, revision,
                    CASE WHEN length(CAST(table_json AS BLOB)) <= ?3 THEN table_json ELSE NULL END,
                    sha256, payload_bytes, row_count, created_at
             FROM reference_table_revisions WHERE name=?1 AND revision=?2",
            params![
                name,
                revision as i64,
                crate::reference_table::MAX_REFERENCE_TABLE_BYTES as i64,
            ],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            },
        )
        .optional()
        .map_err(db)?
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("reference table `{name}` has no revision {revision}"),
            )
        })?;
    if stored_name != name || stored_revision <= 0 || stored_revision as u64 != revision {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "reference table revision identity is corrupt",
        ));
    }
    if payload_bytes < 0 || row_count < 0 || created_at < 0 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "reference table metadata contains a negative value",
        ));
    }
    let table_json = table_json.ok_or_else(|| {
        SparrowError::new(
            ErrorCode::MaxRecordSize,
            "stored reference table payload exceeds the catalog bound",
        )
    })?;
    let table = ReferenceTableSpec::from_json(table_json.as_bytes())?;
    let encoded = table.encoded_bytes()?;
    if encoded.len() as u64 != payload_bytes as u64
        || table.rows.len() as u64 != row_count as u64
    {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "reference table metadata does not match its payload",
        ));
    }
    let computed = reference_table_sha256(name, revision, &table)?;
    if computed != stored_sha || !valid_sha256(&stored_sha) {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            format!("reference table `{name}` revision {revision} sha256 mismatch"),
        ));
    }
    Ok(ReferenceTableRow {
        name: stored_name,
        revision,
        sha256: stored_sha,
        table,
        payload_bytes: payload_bytes as u64,
        row_count: row_count as u64,
        created_at_ms: created_at,
    })
}

fn load_reference_table_metadata(
    c: &Connection,
    name: &str,
    revision: u64,
) -> Result<ReferenceTableMetadata> {
    if revision == 0 || revision > i64::MAX as u64 {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "reference table revision is outside the storage range",
        ));
    }
    let (stored_name, stored_revision, stored_sha, payload_bytes, row_count, created_at): (
        String,
        i64,
        String,
        i64,
        i64,
        i64,
    ) = c
        .query_row(
            "SELECT name, revision, sha256, payload_bytes, row_count, created_at
             FROM reference_table_revisions WHERE name=?1 AND revision=?2",
            params![name, revision as i64],
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
        .optional()
        .map_err(db)?
        .ok_or_else(|| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("reference table `{name}` has no revision {revision}"),
            )
        })?;
    if stored_name != name
        || stored_revision <= 0
        || stored_revision as u64 != revision
        || payload_bytes < 0
        || row_count < 0
        || row_count as u64 > crate::reference_table::MAX_REFERENCE_TABLE_ROWS as u64
        || payload_bytes as u64
            > crate::reference_table::MAX_REFERENCE_TABLE_BYTES as u64
        || created_at < 0
        || !valid_sha256(&stored_sha)
    {
        return Err(SparrowError::new(
            ErrorCode::CodecViolation,
            "reference table metadata is invalid",
        ));
    }
    Ok(ReferenceTableMetadata {
        name: stored_name,
        revision,
        sha256: stored_sha,
        payload_bytes: payload_bytes as u64,
        row_count: row_count as u64,
        created_at_ms: created_at,
    })
}

fn validate_reference_bindings(
    c: &Connection,
    spec: &PipelineSpec,
) -> Result<Vec<ReferenceTableRow>> {
    let mut out = Vec::with_capacity(spec.reference_tables.len());
    for (name, binding) in &spec.reference_tables {
        let row = load_reference_table_revision(c, name, binding.revision)?;
        if row.sha256 != binding.sha256 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "reference table `{name}` revision {} sha256 does not match the pipeline binding",
                    binding.revision
                ),
            )
            .context("expected_sha256", binding.sha256.clone())
            .context("actual_sha256", row.sha256));
        }
        out.push(row);
    }
    Ok(out)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn existing_catalog_schema_version(conn: &Connection) -> Result<u32> {
    let has_meta: i64 = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta')",
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    if has_meta == 0 {
        Ok(0)
    } else {
        meta_u32(conn, "catalog_schema_version")
    }
}

fn init(conn: &Connection) -> Result<()> {
    // Inspect the version before CREATE/ALTER so a future catalog is rejected
    // without mutating it.  Older binaries (schema v2) consequently reject a
    // catalog after this module has committed v3, rather than silently
    // downgrading or ignoring table dependencies.
    let ver = existing_catalog_schema_version(conn)?;
    if ver > CATALOG_SCHEMA_VERSION {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!(
                "catalog_schema_version {ver} is newer than supported {CATALOG_SCHEMA_VERSION}"
            ),
        ));
    }
    // Foreign-key enforcement is a connection setting and must be enabled
    // before the schema transaction.  Every schema object and the version
    // marker below are then committed (or rolled back) as one unit; an
    // interrupted migration cannot leave a half-created v3 catalog behind.
    conn.execute_batch("PRAGMA foreign_keys=ON;")
        .map_err(db)?;
    conn.execute_batch("BEGIN IMMEDIATE").map_err(db)?;
    let initialized = (|| -> Result<()> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS streams (
                name TEXT PRIMARY KEY,
                schema_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pipelines (
                name TEXT PRIMARY KEY,
                latest_revision INTEGER NOT NULL,
                etag TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pipeline_revisions (
                name TEXT NOT NULL,
                revision INTEGER NOT NULL,
                spec_json TEXT NOT NULL,
                etag TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (name, revision)
            );
            -- Status columns stay TEXT (A8). Rust reads/writes PipelineStatus;
            -- no CHECK constraint so existing catalogs keep loading.
            CREATE TABLE IF NOT EXISTS desired_state (
                name TEXT PRIMARY KEY,
                desired_revision INTEGER,
                desired_status TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS actual_state (
                name TEXT PRIMARY KEY,
                actual_revision INTEGER,
                actual_status TEXT NOT NULL,
                attempt_id INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                restart_blocked INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deployment_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT NOT NULL,
                revision INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                detail TEXT,
                restore_claim TEXT NOT NULL DEFAULT 'none'
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at_ms INTEGER NOT NULL,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                target TEXT,
                detail TEXT,
                outcome TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS secrets (
                name TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS allowlist (
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                PRIMARY KEY (host, port)
            );
            "#,
        )
        .map_err(db)?;
        if ver >= 3 && !reference_catalog_schema_complete(conn)? {
            return Err(SparrowError::new(
                ErrorCode::FeatureUnavailable,
                "catalog schema v3 is missing immutable reference-table dependency tables; refusing to continue or GC",
            ));
        }
        if ver>=4 && !plugin_catalog::schema_complete(conn)? {return Err(SparrowError::new(ErrorCode::FeatureUnavailable,"catalog schema v4 is missing plugin reference protection; refusing maintenance"));}
        if ver < CATALOG_SCHEMA_VERSION {
            migrate_actual_consecutive_failures(conn)?;
            migrate_restart_blocked(conn)?;
            migrate_reference_table_catalog(conn)?;
            plugin_catalog::migrate(conn)?;
            if ver == 0 {
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES ('catalog_schema_version', ?1), ('format_version', ?2)",
                    params![CATALOG_SCHEMA_VERSION.to_string(), FORMAT_VERSION.to_string()],
                ).map_err(db)?;
            } else {
                conn.execute(
                    "UPDATE meta SET value=?1 WHERE key='catalog_schema_version'",
                    [CATALOG_SCHEMA_VERSION.to_string()],
                )
                .map_err(db)?;
            }
        }
        Ok(())
    })();
    match initialized {
        Ok(()) => conn.execute_batch("COMMIT").map_err(db),
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn migrate_restart_blocked(conn: &Connection) -> Result<()> {
    let has_latch = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(actual_state)")
            .map_err(db)?;
        let columns = stmt.query_map([], |r| r.get::<_, String>(1)).map_err(db)?;
        let mut found = false;
        for column in columns {
            found |= column.map_err(db)? == "restart_blocked";
        }
        found
    };
    if !has_latch {
        conn.execute_batch(
            "ALTER TABLE actual_state ADD COLUMN restart_blocked INTEGER NOT NULL DEFAULT 0;
             UPDATE actual_state SET restart_blocked=CASE
                WHEN actual_status IN ('running', 'completed') THEN 0
                WHEN actual_status='failed' THEN 1
                ELSE COALESCE((SELECT CASE outcome
                    WHEN 'failed' THEN 1 WHEN 'running' THEN 0 WHEN 'completed' THEN 0 ELSE NULL END
                    FROM deployment_attempts WHERE pipeline=actual_state.name ORDER BY id DESC LIMIT 1),
                    consecutive_failures > 0 OR COALESCE(last_error, '') LIKE 'held:%') END;
             UPDATE actual_state SET last_error=COALESCE(last_error,
                (SELECT detail FROM deployment_attempts WHERE pipeline=actual_state.name ORDER BY id DESC LIMIT 1))
                WHERE restart_blocked=1;"
        ).map_err(db)?;
    }
    Ok(())
}

fn migrate_reference_table_catalog(conn: &Connection) -> Result<()> {
    // Keep the migration idempotent for v0/v1/v2 catalogs and for a process
    // interrupted after the schema objects were created but before meta was
    // advanced.  Data is append-only; there is no destructive backfill.
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS reference_table_revisions (
            name TEXT NOT NULL,
            revision INTEGER NOT NULL,
            table_json TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            payload_bytes INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (name, revision)
         );
         CREATE TABLE IF NOT EXISTS reference_table_heads (
            name TEXT PRIMARY KEY,
            latest_revision INTEGER NOT NULL,
            FOREIGN KEY (name, latest_revision)
                REFERENCES reference_table_revisions(name, revision)
         );
         CREATE TABLE IF NOT EXISTS pipeline_reference_tables (
            pipeline_name TEXT NOT NULL,
            pipeline_revision INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            table_revision INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            PRIMARY KEY (pipeline_name, pipeline_revision, table_name),
            FOREIGN KEY (pipeline_name, pipeline_revision)
                REFERENCES pipeline_revisions(name, revision),
            FOREIGN KEY (table_name, table_revision)
                REFERENCES reference_table_revisions(name, revision)
         );
         CREATE INDEX IF NOT EXISTS pipeline_reference_tables_by_table
            ON pipeline_reference_tables(table_name, table_revision);",
    )
    .map_err(db)?;
    if reference_catalog_schema_complete(conn)? {
        Ok(())
    } else {
        Err(SparrowError::new(
            ErrorCode::InvalidSchema,
            "reference-table catalog migration did not create the complete v3 schema",
        ))
    }
}

fn reference_catalog_schema_complete(conn: &Connection) -> Result<bool> {
    const REVISION_COLUMNS: &[&str] = &[
        "name",
        "revision",
        "table_json",
        "sha256",
        "payload_bytes",
        "row_count",
        "created_at",
    ];
    const HEAD_COLUMNS: &[&str] = &["name", "latest_revision"];
    const PIN_COLUMNS: &[&str] = &[
        "pipeline_name",
        "pipeline_revision",
        "table_name",
        "table_revision",
        "sha256",
    ];
    for (table, columns, primary_key) in [
        (
            "reference_table_revisions",
            REVISION_COLUMNS,
            &["name", "revision"][..],
        ),
        ("reference_table_heads", HEAD_COLUMNS, &["name"][..]),
        (
            "pipeline_reference_tables",
            PIN_COLUMNS,
            &["pipeline_name", "pipeline_revision", "table_name"][..],
        ),
    ] {
        let exists: i64 = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [table],
                |r| r.get(0),
            )
            .map_err(db)?;
        if exists == 0 {
            return Ok(false);
        }
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(db)?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(1))
            .map_err(db)?;
        let mut found = std::collections::HashSet::new();
        for row in rows {
            found.insert(row.map_err(db)?);
        }
        if columns.iter().any(|column| !found.contains(*column)) {
            return Ok(false);
        }
        if !primary_key_matches(conn, table, primary_key)? {
            return Ok(false);
        }
    }
    // These relations are what make a committed pipeline revision a durable
    // GC root.  Merely having the columns is not enough: a damaged v3
    // catalog with a missing/mismatched FK must fail closed rather than let
    // a later DELETE silently orphan a running pipeline's lookup revision.
    if !primary_key_matches(conn, "pipeline_revisions", &["name", "revision"])? {
        return Ok(false);
    }
    if !foreign_key_matches(
        conn,
        "reference_table_heads",
        "reference_table_revisions",
        &["name", "latest_revision"],
        &["name", "revision"],
    )? || !foreign_key_matches(
        conn,
        "pipeline_reference_tables",
        "pipeline_revisions",
        &["pipeline_name", "pipeline_revision"],
        &["name", "revision"],
    )? || !foreign_key_matches(
        conn,
        "pipeline_reference_tables",
        "reference_table_revisions",
        &["table_name", "table_revision"],
        &["name", "revision"],
    )? {
        return Ok(false);
    }
    Ok(true)
}

fn primary_key_matches(conn: &Connection, table: &str, expected: &[&str]) -> Result<bool> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(db)?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(5)?)))
        .map_err(db)?;
    let mut actual = Vec::new();
    for row in rows {
        let (name, position) = row.map_err(db)?;
        if position > 0 {
            actual.push((position, name));
        }
    }
    actual.sort_by_key(|(position, _)| *position);
    Ok(actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|((_, actual), expected)| actual.as_str() == *expected))
}

fn foreign_key_matches(
    conn: &Connection,
    table: &str,
    target: &str,
    from: &[&str],
    to: &[&str],
) -> Result<bool> {
    if from.len() != to.len() {
        return Ok(false);
    }
    let mut stmt = conn
        .prepare(&format!("PRAGMA foreign_key_list({table})"))
        .map_err(db)?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })
        .map_err(db)?;
    let mut groups = std::collections::BTreeMap::<
        i64,
        (String, Vec<(i64, String, String)>),
    >::new();
    for row in rows {
        let (id, sequence, target_table, source, destination) = row.map_err(db)?;
        let entry = groups
            .entry(id)
            .or_insert_with(|| (target_table, Vec::new()));
        entry.1.push((sequence, source, destination));
    }
    groups.values_mut().for_each(|(_, pairs)| {
        pairs.sort_by_key(|(sequence, _, _)| *sequence);
    });
    let matches = groups.values().any(|(target_table, pairs)| {
        target_table.as_str() == target
            && pairs.len() == from.len()
            && pairs
                .iter()
                .zip(from.iter().zip(to))
                .all(|((_, actual_from, actual_to), (expected_from, expected_to))| {
                    actual_from.as_str() == *expected_from
                        && actual_to.as_str() == *expected_to
                })
    });
    Ok(matches)
}

fn migrate_actual_consecutive_failures(conn: &Connection) -> Result<()> {
    let mut has_cf = false;
    {
        let mut stmt = conn
            .prepare("PRAGMA table_info(actual_state)")
            .map_err(db)?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(1)).map_err(db)?;
        for col in rows {
            if col.map_err(db)? == "consecutive_failures" {
                has_cf = true;
                break;
            }
        }
    }
    if !has_cf {
        conn.execute(
            "ALTER TABLE actual_state ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(db)?;
    }
    Ok(())
}

fn meta_u32(conn: &Connection, key: &str) -> Result<u32> {
    let s: String = conn
        .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
        .map_err(db)?;
    s.parse()
        .map_err(|_| SparrowError::new(ErrorCode::InvalidSchema, "meta version"))
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn check_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "name must be 1..=64 characters",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "name may contain only [A-Za-z0-9_.-]",
        ));
    }
    Ok(())
}

fn db(err: rusqlite::Error) -> SparrowError {
    SparrowError::new(ErrorCode::Internal, format!("catalog: {err}"))
}

/// Threat model: catalog file is trusted-host local state. Secrets are
/// sealed with ChaCha20-Poly1305 (`enc:v2:`). Key comes from
/// `SPARROW_SECRETS_KEY` / `SPARROW_SECRETS_KEY_FILE` (32 bytes or 64 hex
/// chars). Without those, a process-local random key is used (dev only)
/// and a warning is logged at store open (N12). `--safe-mode` /
/// `SPARROW_SAFE_MODE=1` / `SPARROW_REQUIRE_SECRETS_KEY=1` refuse instead.
/// Unprefixed plaintext is rejected (no fallback). Not a KMS. File mode 0600.
pub fn secrets_key_configured() -> bool {
    env_nonempty("SPARROW_SECRETS_KEY_FILE") || env_nonempty("SPARROW_SECRETS_KEY")
}

pub fn secrets_key_required() -> bool {
    std::env::var("SPARROW_REQUIRE_SECRETS_KEY").ok().as_deref() == Some("1")
        || std::env::var("SPARROW_SAFE_MODE").ok().as_deref() == Some("1")
}

fn env_nonempty(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|s| !s.is_empty())
        .unwrap_or(false)
}

/// Decision used at store open and when sealing. `configured` / `required`
/// are injected in tests so we do not mutate process env.
pub(crate) fn secrets_key_on_open(configured: bool, required: bool) -> Result<SecretsKeyOnOpen> {
    if configured {
        return Ok(SecretsKeyOnOpen::Configured);
    }
    if required {
        return Err(SparrowError::new(
            ErrorCode::SecretMissing,
            "SPARROW_SECRETS_KEY or SPARROW_SECRETS_KEY_FILE is required in safe/production mode",
        ));
    }
    Ok(SecretsKeyOnOpen::ProcessLocalDev)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SecretsKeyOnOpen {
    Configured,
    ProcessLocalDev,
}

fn note_secrets_key_on_open() -> Result<()> {
    match secrets_key_on_open(secrets_key_configured(), secrets_key_required())? {
        SecretsKeyOnOpen::Configured => Ok(()),
        SecretsKeyOnOpen::ProcessLocalDev => {
            tracing::warn!(
                "SPARROW_SECRETS_KEY / SPARROW_SECRETS_KEY_FILE is unset; using a process-local random key (dev only). Sealed secrets will not survive restart. Set a key, or SPARROW_SAFE_MODE=1 / SPARROW_REQUIRE_SECRETS_KEY=1 to refuse."
            );
            Ok(())
        }
    }
}

fn secrets_key() -> Result<[u8; 32]> {
    if let Ok(path) = std::env::var("SPARROW_SECRETS_KEY_FILE") {
        if !path.is_empty() {
            let raw = std::fs::read(path.trim()).map_err(|e| {
                SparrowError::new(ErrorCode::SecretMissing, format!("secrets key file: {e}"))
            })?;
            return parse_secrets_key(&raw);
        }
    }
    if let Ok(s) = std::env::var("SPARROW_SECRETS_KEY") {
        if !s.is_empty() {
            return parse_secrets_key(s.as_bytes());
        }
    }
    let _ = secrets_key_on_open(false, secrets_key_required())?;
    Ok(*PROCESS_SECRETS_KEY.get_or_init(random_key))
}

fn parse_secrets_key(raw: &[u8]) -> Result<[u8; 32]> {
    let trimmed = std::str::from_utf8(raw).unwrap_or("").trim();
    if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        let v = hex_decode(trimmed).map_err(|_| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "SPARROW_SECRETS_KEY hex is invalid",
            )
        })?;
        return v.try_into().map_err(|_| {
            SparrowError::new(
                ErrorCode::InvalidArgument,
                "SPARROW_SECRETS_KEY must be 32 bytes",
            )
        });
    }
    if raw.len() == 32 {
        let mut k = [0u8; 32];
        k.copy_from_slice(raw);
        return Ok(k);
    }
    let t = trimmed.as_bytes();
    if t.len() == 32 {
        let mut k = [0u8; 32];
        k.copy_from_slice(t);
        return Ok(k);
    }
    Err(SparrowError::new(
        ErrorCode::InvalidArgument,
        "SPARROW_SECRETS_KEY must be 32 raw bytes or 64 hex characters",
    ))
}

fn random_key() -> [u8; 32] {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut k = [0u8; 32];
    let _ = SystemRandom::new().fill(&mut k);
    k
}

static PROCESS_SECRETS_KEY: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();

fn seal_secret(plain: &str) -> Result<String> {
    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
    use ring::rand::{SecureRandom, SystemRandom};
    let key = secrets_key()?;
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, &key)
        .map_err(|_| SparrowError::new(ErrorCode::Internal, "secrets AEAD key rejected"))?;
    let aead = LessSafeKey::new(unbound);
    let mut nonce_bytes = [0u8; 12];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| SparrowError::new(ErrorCode::Internal, "secrets nonce failed"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut in_out = plain.as_bytes().to_vec();
    aead.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| SparrowError::new(ErrorCode::Internal, "secrets seal failed"))?;
    let mut blob = Vec::with_capacity(12 + in_out.len());
    blob.extend_from_slice(&nonce_bytes);
    blob.extend_from_slice(&in_out);
    Ok(format!("enc:v2:{}", hex_encode(&blob)))
}

fn unseal_secret(stored: &str) -> Result<String> {
    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
    if let Some(hex) = stored.strip_prefix("enc:v2:") {
        let raw = hex_decode(hex)
            .map_err(|_| SparrowError::new(ErrorCode::InvalidSchema, "stored secret is corrupt"))?;
        if raw.len() < 12 + 16 {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "stored secret is truncated",
            ));
        }
        let key = secrets_key()?;
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, &key)
            .map_err(|_| SparrowError::new(ErrorCode::Internal, "secrets AEAD key rejected"))?;
        let aead = LessSafeKey::new(unbound);
        let nonce = Nonce::try_assume_unique_for_key(&raw[..12]).map_err(|_| {
            SparrowError::new(ErrorCode::InvalidSchema, "stored secret nonce is invalid")
        })?;
        let mut in_out = raw[12..].to_vec();
        let pt = aead
            .open_in_place(nonce, Aad::empty(), &mut in_out)
            .map_err(|_| {
                SparrowError::new(ErrorCode::InvalidSchema, "stored secret failed AEAD open")
            })?;
        String::from_utf8(pt.to_vec())
            .map_err(|_| SparrowError::new(ErrorCode::InvalidSchema, "stored secret is not utf8"))
    } else if stored.starts_with("enc:v1:") {
        Err(SparrowError::new(
            ErrorCode::InvalidSchema,
            "legacy XOR secret envelope is no longer accepted; reseal as enc:v2",
        ))
    } else {
        Err(SparrowError::new(
            ErrorCode::PolicyDenied,
            "unprefixed plaintext secrets are rejected (safe/production and default)",
        ))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 {
        return Err(());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let b = s.as_bytes();
    for i in (0..b.len()).step_by(2) {
        let hi = hex_val(b[i])?;
        let lo = hex_val(b[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_val(c: u8) -> std::result::Result<u8, ()> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{PipelineSpec, SinkSpec, SourceSpec};

    const R9_SCHEMA: &str = r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}"#;
    fn r9_file_spec() -> PipelineSpec {
        let mut s = spec();
        s.source.kind = "file".into();
        s.source.path = Some("fixture.ndjson".into());
        s.sql = Some(
            "SELECT device_id, SUM(v) AS s FROM sensors GROUP BY device_id, COUNT_WINDOW(2)".into(),
        );
        s
    }
    fn r9_misses(store: &Store) -> usize {
        store.inner.status_effective.lock().unwrap().misses
    }

    #[test]
    fn r9_status_cache_hits_and_invalidates_schema_revision_and_failed_binding() {
        let s = Store::open_memory().unwrap();
        s.put_stream("sensors", R9_SCHEMA).unwrap();
        let mut spec = r9_file_spec();
        s.put_pipeline("cached", &spec, None).unwrap();
        assert_eq!(
            s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"],
            true
        );
        for _ in 0..20 {
            assert_eq!(
                s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"],
                true
            );
        }
        assert_eq!(r9_misses(&s), 1);
        s.put_stream(
            "sensors",
            r#"{"fields":[{"name":"wrong","type":"int64","nullable":false}]}"#,
        )
        .unwrap();
        let (row, failed) = s.effective_pipeline_status("cached").unwrap();
        assert_eq!(
            row.latest_revision, 1,
            "schema changes do not bump pipeline revision"
        );
        assert!(failed["aligned_eligible"].is_null());
        assert!(failed["aligned_eligibility_reason"]
            .as_str()
            .unwrap()
            .starts_with("plan_bind_failed:"));
        s.effective_pipeline_status("cached").unwrap();
        assert_eq!(r9_misses(&s), 2);
        s.put_stream("sensors", R9_SCHEMA).unwrap();
        assert_eq!(
            s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"],
            true
        );
        spec.sql = Some("SELECT device_id FROM sensors".into());
        s.put_pipeline("cached", &spec, Some("rev-1")).unwrap();
        let (row, effective) = s.effective_pipeline_status("cached").unwrap();
        assert_eq!(row.latest_revision, 2);
        assert_eq!(effective["aligned_eligible"], true);
        assert_eq!(effective["aligned_eligibility_reason"], "zero_state_file_cut");
        assert_eq!(r9_misses(&s), 4);
        assert_eq!(s.inner.status_effective.lock().unwrap().entries.len(), 1);
    }

    #[test]
    fn r9_status_cache_is_bounded_and_does_not_cross_stores() {
        let s = Store::open_memory().unwrap();
        s.put_stream("sensors", R9_SCHEMA).unwrap();
        for n in 0..STATUS_EFFECTIVE_CACHE_CAP + 10 {
            let name = format!("p{n}");
            s.put_pipeline(&name, &r9_file_spec(), None).unwrap();
            s.effective_pipeline_status(&name).unwrap();
        }
        assert_eq!(
            s.inner.status_effective.lock().unwrap().entries.len(),
            STATUS_EFFECTIVE_CACHE_CAP
        );
        let other = Store::open_memory().unwrap();
        other.put_pipeline("p0", &r9_file_spec(), None).unwrap();
        assert!(other.effective_pipeline_status("p0").unwrap().1["aligned_eligible"].is_null());
        assert_eq!(r9_misses(&other), 1);
    }

    #[test]
    fn r9_status_cache_invalidates_other_connection_and_failed_transaction() {
        let path = std::env::temp_dir().join(format!(
            "sparrow-r9-status-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let s = Store::open(&path).unwrap();
        s.put_stream("sensors", R9_SCHEMA).unwrap();
        s.put_pipeline("cached", &r9_file_spec(), None).unwrap();
        assert_eq!(
            s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"],
            true
        );
        let other = Store::open(&path).unwrap();
        other
            .put_stream(
                "sensors",
                r#"{"fields":[{"name":"wrong","type":"int64","nullable":false}]}"#,
            )
            .unwrap();
        assert!(s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"].is_null());
        other.put_stream("sensors", R9_SCHEMA).unwrap();
        assert_eq!(
            s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"],
            true
        );
        s.inner.fail_before_commit.store(true, Ordering::SeqCst);
        assert!(s.put_stream("sensors", "{}").is_err());
        assert_eq!(
            s.effective_pipeline_status("cached").unwrap().1["aligned_eligible"],
            true
        );
        drop(other);
        drop(s);
        std::fs::remove_file(path).unwrap();
    }

    fn spec() -> PipelineSpec {
        PipelineSpec {
            graph_io: None,
            version: 1,
            stream: "sensors".into(),
            reference_tables: Default::default(),
            external_lookups: Default::default(),
            sql: Some("SELECT device_id FROM sensors".into()),
            graph: None,
            source: SourceSpec {
                plugin: None,
                jetstream: None,
                http_poll: None,
                nats: None,
                databus: None,
                kind: "mqtt".into(),
                host: Some("127.0.0.1".into()),
                port: Some(1883),
                topic: "sensors/json".into(),
                client_id: Some("t".into()),
                qos: 0,
                clean_session: true,
                username_secret: None,
                password_secret: None,
                skip_verify: false,
                inbox_capacity: 8,
                inbox_wait_ms: None,
                tcp_quickack: None,
                inbox_bytes: None,
                use_demo_io: false,
                bind: None,
                path: None,
                tls: false,
                file_contract: None,
            },
            sink: SinkSpec {
                nats: None,
                databus: None,
                influxdb: None,
                jetstream: None,
                plugin: None,
                action: None,
                file: None,
                kind: "http".into(),
                url: Some("http://127.0.0.1:9/ingest".into()),
                skip_verify: false,
                outbox_capacity: 8,
                batch_rows: None,
                batch_bytes: None,
                linger_ms: None,
                max_inflight: None,
                use_demo_io: false,
                header_secret: None,
                host: None,
                port: None,
                topic: None,
                client_id: None,
                qos: 0,
                clean_session: true,
                tls: false,
            },
            delivery: "live_best_effort".into(),
            recovery: "restart_fresh".into(),
            restore: None,
            checkpoint_dir: None,
            checkpoint: None,
            fail_on_decode: false,
        }
    }

    #[test]
    fn r22_put_pipeline_is_one_transaction() {
        let s = Store::open_memory().unwrap();
        s.put_pipeline("hot", &spec(), None).unwrap();
        assert!(s.get_pipeline_revision("hot", 1).is_ok());
        s.put_pipeline("hot", &spec(), Some("rev-1")).unwrap();
        assert_eq!(s.get_pipeline("hot").unwrap().latest_revision, 2);
        assert!(s.get_pipeline_revision("hot", 1).is_ok());
        assert!(s.get_pipeline_revision("hot", 2).is_ok());
    }

    #[test]
    fn r23_desired_revision_spec_is_not_latest() {
        let s = Store::open_memory().unwrap();
        let mut a = spec();
        a.sql = Some("SELECT device_id FROM sensors".into());
        s.put_pipeline("hot", &a, None).unwrap();
        let mut b = spec();
        b.sql = Some("SELECT temperature FROM sensors".into());
        s.put_pipeline("hot", &b, Some("rev-1")).unwrap();
        let desired = s.get_pipeline_revision("hot", 1).unwrap();
        assert_eq!(
            desired.spec.sql.as_deref(),
            Some("SELECT device_id FROM sensors")
        );
        let latest = s.get_pipeline("hot").unwrap();
        assert_eq!(
            latest.spec.sql.as_deref(),
            Some("SELECT temperature FROM sensors")
        );
        assert_ne!(desired.spec.sql, latest.spec.sql);
    }

    #[test]
    fn n12_unconfigured_secrets_key_warns_or_refuses() {
        assert_eq!(
            secrets_key_on_open(false, false).unwrap(),
            SecretsKeyOnOpen::ProcessLocalDev
        );
        assert_eq!(
            secrets_key_on_open(true, false).unwrap(),
            SecretsKeyOnOpen::Configured
        );
        assert_eq!(
            secrets_key_on_open(true, true).unwrap(),
            SecretsKeyOnOpen::Configured
        );
        let err = secrets_key_on_open(false, true).unwrap_err();
        assert_eq!(err.code, ErrorCode::SecretMissing);
        assert!(
            err.message.contains("SPARROW_SECRETS_KEY"),
            "{}",
            err.message
        );
        // Dev open still works (warning is tracing-only).
        let s = Store::open_memory().unwrap();
        s.put_secret("n12", "dev-only").unwrap();
        assert_eq!(s.get_secret("n12").unwrap().as_deref(), Some("dev-only"));
    }

    #[test]
    fn r27_secrets_are_sealed_and_not_plaintext() {
        let s = Store::open_memory().unwrap();
        s.put_secret("pw", "super-secret-value").unwrap();
        let raw = s.debug_raw_secret("pw").unwrap();
        assert!(
            raw.starts_with("enc:v2:"),
            "stored secret must be AEAD-sealed: {raw}"
        );
        assert!(!raw.contains("super-secret-value"));
        assert_eq!(
            s.get_secret("pw").unwrap().as_deref(),
            Some("super-secret-value")
        );
        let err = unseal_secret("super-secret-value").unwrap_err();
        assert_eq!(err.code, ErrorCode::PolicyDenied);
        let too_big = "x".repeat(4097);
        assert_eq!(
            s.put_secret("big", &too_big).unwrap_err().code,
            ErrorCode::MaxRecordSize
        );
    }

    #[test]
    fn r22_crash_before_commit_keeps_old_revision() {
        let s = Store::open_memory().unwrap();
        s.put_pipeline("hot", &spec(), None).unwrap();
        assert_eq!(s.get_pipeline("hot").unwrap().latest_revision, 1);
        s.debug_fail_next_commit();
        let err = s.put_pipeline("hot", &spec(), Some("rev-1")).unwrap_err();
        assert!(err.message.contains("injected") || err.message.contains("crash"));
        let row = s.get_pipeline("hot").unwrap();
        assert_eq!(row.latest_revision, 1, "partial write must roll back");
        assert!(s.get_pipeline_revision("hot", 2).is_err());
        s.put_pipeline("hot", &spec(), Some("rev-1")).unwrap();
        assert_eq!(s.get_pipeline("hot").unwrap().latest_revision, 2);
        assert!(s.get_pipeline_revision("hot", 1).is_ok());
        assert!(s.get_pipeline_revision("hot", 2).is_ok());
    }

    #[tokio::test]
    async fn r26_slow_catalog_does_not_freeze_runtime() {
        let store = Store::open_memory().unwrap();
        let progressed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = progressed.clone();
        let slow = store.run_blocking(|| {
            std::thread::sleep(std::time::Duration::from_millis(250));
            Ok(())
        });
        let tick = async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        };
        let start = std::time::Instant::now();
        let (slow_r, _) = tokio::join!(slow, tick);
        slow_r.unwrap();
        assert!(
            progressed.load(std::sync::atomic::Ordering::SeqCst),
            "sibling async work must proceed while catalog is on the blocking pool"
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
    }

    #[tokio::test]
    async fn r26_catalog_runs_off_async_worker() {
        let store = Store::open_memory().unwrap();
        let async_tid = std::thread::current().id();
        let catalog_tid = store
            .run_blocking({
                let store = store.clone();
                move || {
                    store.put_stream("sensors", "{}")?;
                    Ok(std::thread::current().id())
                }
            })
            .await
            .unwrap();
        assert_ne!(
            async_tid, catalog_tid,
            "SQLite must not run on the async worker thread"
        );
        assert_eq!(store.get_stream("sensors").unwrap().name, "sensors");
    }

    #[test]
    fn self_review_restore_revision_and_activation_commit_or_rollback_together() {
        let s = Store::open_memory().unwrap();
        let mut spec = r9_file_spec();
        s.put_pipeline("restore", &spec, None).unwrap();
        s.request_start_revision("restore", 1).unwrap();
        s.set_actual("restore", "failed", Some(1), 7, Some("prior failure"))
            .unwrap();
        spec.restore = Some(crate::RestoreSpec {
            kind: "checkpoint".into(),
            snapshot_id: Some("42".into()),
            client_id: None,
        });
        s.debug_fail_next_commit();
        assert!(s
            .put_pipeline_and_start("restore", &spec, Some("rev-1"))
            .is_err());
        assert_eq!(s.get_pipeline("restore").unwrap().latest_revision, 1);
        assert!(s.get_pipeline("restore").unwrap().spec.restore.is_none());
        assert!(s.get_pipeline_revision("restore", 2).is_err());
        assert_eq!(s.desired("restore").unwrap().revision, Some(1));
        assert_eq!(s.actual("restore").unwrap().status, "failed");
        assert!(s.actual("restore").unwrap().restart_blocked);
        let row = s
            .put_pipeline_and_start("restore", &spec, Some("rev-1"))
            .unwrap();
        assert_eq!(row.latest_revision, 2);
        assert_eq!(s.desired("restore").unwrap().revision, Some(2));
        assert_eq!(s.desired("restore").unwrap().status, "running");
        assert_eq!(s.actual("restore").unwrap().status, "stopped");
        assert!(!s.actual("restore").unwrap().restart_blocked);
        assert_eq!(
            s.get_pipeline_revision("restore", 2)
                .unwrap()
                .spec
                .restore
                .unwrap()
                .snapshot_id
                .as_deref(),
            Some("42")
        );
        assert!(s
            .put_pipeline_and_start("restore", &spec, Some("rev-1"))
            .is_err());
        assert_eq!(s.get_pipeline("restore").unwrap().latest_revision, 2);
        assert_eq!(s.desired("restore").unwrap().revision, Some(2));
    }

    #[test]
    fn r10_process_restart_holds_fixed_replay_until_explicit_start() {
        let s = Store::open_memory().unwrap();
        let mut spec = r9_file_spec();
        spec.restore = Some(crate::RestoreSpec {
            kind: "checkpoint".into(),
            snapshot_id: Some("1".into()),
            client_id: None,
        });
        spec.checkpoint = Some(crate::CheckpointSpec {
            resume_latest: true,
            ..Default::default()
        });
        s.put_pipeline_and_start("fixed", &spec, None).unwrap();
        s.set_actual("fixed", "running", Some(1), 1, None).unwrap();
        s.reset_actual_after_process_restart().unwrap();
        assert!(s.actual("fixed").unwrap().restart_blocked);
        assert!(s
            .actual("fixed")
            .unwrap()
            .last_error
            .unwrap()
            .contains("fixed snapshot"));
        s.request_start_revision("fixed", 1).unwrap();
        assert!(!s.actual("fixed").unwrap().restart_blocked);
        assert_eq!(
            s.get_pipeline("fixed").unwrap().spec.fixed_snapshot_id(),
            Some(1)
        );
    }

    #[test]
    fn r6_start_preserves_only_matching_running_revision() {
        for status in [
            "stopped",
            "starting",
            "waiting",
            "failed",
            "completed",
            "running",
        ] {
            let s = Store::open_memory().unwrap();
            s.put_pipeline("hot", &spec(), None).unwrap();
            // Desired running alone must not preserve a failed/waiting actual row.
            s.set_desired("hot", "running", Some(1)).unwrap();
            s.set_actual("hot", "failed", Some(1), 6, Some("previous fault"))
                .unwrap();
            s.set_actual("hot", status, Some(1), 7, Some("test status"))
                .unwrap();
            s.request_start_revision("hot", 1).unwrap();
            let actual = s.actual("hot").unwrap();
            assert_eq!(
                actual.status.as_str(),
                if status == "running" {
                    "running"
                } else {
                    "stopped"
                }
            );
            assert_eq!(
                actual.revision,
                if status == "running" { Some(1) } else { None }
            );
            assert_eq!(actual.attempt_id, 7);
            assert_eq!(actual.consecutive_failures, 0);
            assert!(!actual.restart_blocked);
            assert!(actual.last_error.is_none());
        }

        let s = Store::open_memory().unwrap();
        s.put_pipeline("hot", &spec(), None).unwrap();
        s.put_pipeline("hot", &spec(), Some("rev-1")).unwrap();
        s.set_desired("hot", "running", Some(1)).unwrap();
        s.set_actual("hot", "running", Some(1), 7, None).unwrap();
        s.debug_fail_next_commit();
        assert!(s.request_start_revision("hot", 2).is_err());
        assert_eq!(s.desired("hot").unwrap().revision, Some(1));
        assert_eq!(s.actual("hot").unwrap().status, "running");
        assert_eq!(s.actual("hot").unwrap().revision, Some(1));
        s.request_start_revision("hot", 2).unwrap();
        assert_eq!(s.desired("hot").unwrap().revision, Some(2));
        assert_eq!(s.actual("hot").unwrap().status, "stopped");
        assert_eq!(s.actual("hot").unwrap().revision, None);
    }

    #[test]
    fn r5_failure_latch_and_explicit_start_are_transactional() {
        let s = Store::open_memory().unwrap();
        s.put_pipeline("hot", &spec(), None).unwrap();
        s.debug_fail_next_commit();
        assert!(s
            .set_actual("hot", "failed", Some(1), 1, Some("fault"))
            .is_err());
        assert!(!s.actual("hot").unwrap().restart_blocked);
        assert_eq!(s.actual("hot").unwrap().status, "stopped");

        s.set_actual("hot", "failed", Some(1), 1, Some("fault"))
            .unwrap();
        s.insert_attempt("hot", 1, "failed", Some("fault")).unwrap();
        assert!(s.actual("hot").unwrap().restart_blocked);
        s.debug_fail_next_commit();
        assert!(s.request_start_revision("hot", 1).is_err());
        assert_eq!(s.desired("hot").unwrap().status, "stopped");
        assert_eq!(s.actual("hot").unwrap().status, "failed");
        assert!(s.actual("hot").unwrap().restart_blocked);
        s.reset_consecutive_failures("hot").unwrap();
        assert!(
            s.actual("hot").unwrap().restart_blocked,
            "crash count is not the safe-mode latch"
        );
        s.reset_actual_after_process_restart().unwrap();
        assert!(s.actual("hot").unwrap().restart_blocked);

        s.request_start_revision("hot", 1).unwrap();
        assert_eq!(s.desired("hot").unwrap().status, "running");
        assert_eq!(s.actual("hot").unwrap().status, "stopped");
        assert!(!s.actual("hot").unwrap().restart_blocked);
        assert_eq!(s.actual("hot").unwrap().consecutive_failures, 0);
        assert!(s.actual("hot").unwrap().last_error.is_none());
        assert_eq!(
            s.last_attempt("hot").unwrap().unwrap().outcome,
            "failed",
            "unlock must not require destroying historical failure evidence"
        );
    }

    fn legacy_v1_catalog() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta VALUES ('catalog_schema_version', '1'), ('format_version', '1');
             CREATE TABLE actual_state(name TEXT PRIMARY KEY, actual_revision INTEGER,
                actual_status TEXT NOT NULL, attempt_id INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0, last_error TEXT, updated_at INTEGER NOT NULL);
             CREATE TABLE deployment_attempts(id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT NOT NULL, revision INTEGER NOT NULL, started_at INTEGER NOT NULL,
                outcome TEXT NOT NULL, detail TEXT, restore_claim TEXT NOT NULL DEFAULT 'none');
             INSERT INTO actual_state VALUES
                ('a', 1, 'failed', 1, 1, 'actual failure', 0),
                ('b', 1, 'stopped', 1, 0, NULL, 0),
                ('c', 1, 'stopped', 1, 1, 'failure with evicted history', 0),
                ('d', 1, 'running', 2, 1, NULL, 0),
                ('e', 1, 'stopped', 2, 1, NULL, 0),
                ('f', 1, 'completed', 2, 0, NULL, 0),
                ('g', 1, 'stopped', 1, 0, 'held: safe-mode and last attempt failed', 0);
             INSERT INTO deployment_attempts(pipeline, revision, started_at, outcome, detail) VALUES
                ('b', 1, 0, 'failed', 'history-only failure'), ('e', 1, 0, 'running', NULL);"
        ).unwrap();
        c
    }

    #[test]
    fn r5_v1_migration_preserves_failures_and_does_not_rearm_unlocked_jobs() {
        let c = legacy_v1_catalog();
        init(&c).unwrap();
        assert_eq!(meta_u32(&c, "catalog_schema_version").unwrap(), CATALOG_SCHEMA_VERSION);
        assert_eq!(meta_u32(&c, "format_version").unwrap(), 1);
        for (name, expected) in [
            ("a", true),
            ("b", true),
            ("c", true),
            ("d", false),
            ("e", false),
            ("f", false),
            ("g", true),
        ] {
            let blocked: bool = c
                .query_row(
                    "SELECT restart_blocked FROM actual_state WHERE name=?1",
                    [name],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(blocked, expected, "migration for {name}");
        }
        let error: String = c
            .query_row(
                "SELECT last_error FROM actual_state WHERE name='b'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(error, "history-only failure");
        c.execute(
            "UPDATE actual_state SET restart_blocked=0 WHERE name='b'",
            [],
        )
        .unwrap();
        init(&c).unwrap();
        let blocked: bool = c
            .query_row(
                "SELECT restart_blocked FROM actual_state WHERE name='b'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            !blocked,
            "v2 reopen must not reconstruct a cleared latch from stale history"
        );
    }

    #[test]
    fn r5_failed_migration_rolls_back_column_and_version() {
        let c = legacy_v1_catalog();
        c.execute_batch(
            "CREATE TRIGGER fail_migration BEFORE UPDATE ON actual_state
            BEGIN SELECT RAISE(ABORT, 'injected migration failure'); END;",
        )
        .unwrap();
        assert!(init(&c).is_err());
        assert_eq!(meta_u32(&c, "catalog_schema_version").unwrap(), 1);
        assert!(c
            .prepare("SELECT restart_blocked FROM actual_state")
            .is_err());
        c.execute_batch("DROP TRIGGER fail_migration;").unwrap();
        init(&c).unwrap();
        assert_eq!(meta_u32(&c, "catalog_schema_version").unwrap(), CATALOG_SCHEMA_VERSION);
    }

    #[test]
    fn put_get_and_if_match() {
        let s = Store::open_memory().unwrap();
        assert_eq!(s.schema_version().unwrap(), CATALOG_SCHEMA_VERSION);
        let row = s.put_pipeline("hot", &spec(), None).unwrap();
        assert_eq!(row.etag, "rev-1");
        let err = s.put_pipeline("hot", &spec(), None).unwrap_err();
        assert!(err.message.contains("If-Match"));
        let row2 = s.put_pipeline("hot", &spec(), Some("rev-1")).unwrap();
        assert_eq!(row2.etag, "rev-2");
        s.set_desired("hot", "running", Some(2)).unwrap();
        assert_eq!(s.desired("hot").unwrap().status, "running");
        s.reset_actual_after_process_restart().unwrap();
        assert_eq!(s.actual("hot").unwrap().status, "stopped");
    }

    #[test]
    fn a8_unknown_status_string_is_rejected() {
        let s = Store::open_memory().unwrap();
        s.put_pipeline("hot", &spec(), None).unwrap();
        let err = s.set_desired("hot", "pretty_please", Some(1)).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("pretty_please"), "{}", err.message);
        let err = s.set_desired("hot", "failed", Some(1)).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(
            err.message.contains("running or stopped"),
            "{}",
            err.message
        );
        let err = s
            .set_actual("hot", "on-fire", Some(1), 1, None)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }
}

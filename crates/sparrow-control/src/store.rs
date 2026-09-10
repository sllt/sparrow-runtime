//! SQLite catalog. A committed desired-state change does **not** wait for
//! MQTT/HTTP to come up — the supervisor converges afterwards.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use sparrow_model::{ErrorCode, Result, SparrowError};

use crate::spec::PipelineSpec;

pub const CATALOG_SCHEMA_VERSION: u32 = 1;
pub const FORMAT_VERSION: u32 = 1;
const AUDIT_CAP: usize = 200;
const ATTEMPT_CAP: usize = 100;

/// Catalog handle. SQLite lives behind a mutex and is executed on the
/// bounded blocking pool (`run_blocking`) from async paths (R26).
#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    conn: Mutex<Connection>,
    fail_before_commit: AtomicBool,
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
    pub status: String,
}

#[derive(Clone, Debug)]
pub struct ActualState {
    pub name: String,
    pub revision: Option<u64>,
    pub status: String,
    pub attempt_id: u64,
    pub consecutive_failures: u64,
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
        Ok(Self {
            inner: Arc::new(StoreInner {
                conn: Mutex::new(conn),
                fail_before_commit: AtomicBool::new(false),
            }),
        })
    }

    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(db)?;
        init(&conn)?;
        Ok(Self {
            inner: Arc::new(StoreInner {
                conn: Mutex::new(conn),
                fail_before_commit: AtomicBool::new(false),
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
        self.read(|c| {
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
        })
    }

    pub fn put_pipeline(
        &self,
        name: &str,
        spec: &PipelineSpec,
        expected_etag: Option<&str>,
    ) -> Result<PipelineRow> {
        check_name(name)?;
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
                params![name, revision.map(|r| r as i64), status, now_ms()],
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
                "running" | "completed" => 0,
                "failed" => prev.saturating_add(1),
                _ => prev,
            };
            c.execute(
                "INSERT INTO actual_state(name, actual_revision, actual_status, attempt_id, consecutive_failures, last_error, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(name) DO UPDATE SET
                    actual_revision=excluded.actual_revision,
                    actual_status=excluded.actual_status,
                    attempt_id=excluded.attempt_id,
                    consecutive_failures=excluded.consecutive_failures,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at",
                params![
                    name,
                    revision.map(|r| r as i64),
                    status,
                    attempt_id as i64,
                    consecutive,
                    last_error,
                    now_ms()
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

    /// Clear the consecutive-failure hold so `request_start` can converge again.
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

    pub fn desired(&self, name: &str) -> Result<DesiredState> {
        self.read(|c| {
            c.query_row(
                "SELECT name, desired_revision, desired_status FROM desired_state WHERE name=?1",
                [name],
                |r| {
                    Ok(DesiredState {
                        name: r.get(0)?,
                        revision: r.get::<_, Option<i64>>(1)?.map(|v| v as u64),
                        status: r.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(db)?
            .ok_or_else(|| {
                SparrowError::new(
                    ErrorCode::InvalidArgument,
                    format!("no desired state for `{name}`"),
                )
            })
        })
    }

    pub fn actual(&self, name: &str) -> Result<ActualState> {
        self.read(|c| {
            c.query_row(
                "SELECT name, actual_revision, actual_status, attempt_id, COALESCE(consecutive_failures, 0), last_error FROM actual_state WHERE name=?1",
                [name],
                |r| {
                    Ok(ActualState {
                        name: r.get(0)?,
                        revision: r.get::<_, Option<i64>>(1)?.map(|v| v as u64),
                        status: r.get(2)?,
                        attempt_id: r.get::<_, i64>(3)? as u64,
                        consecutive_failures: r.get::<_, i64>(4)? as u64,
                        last_error: r.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(db)?
            .ok_or_else(|| {
                SparrowError::new(ErrorCode::InvalidArgument, format!("no actual state for `{name}`"))
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
                    Ok(DesiredState {
                        name: r.get(0)?,
                        revision: r.get::<_, Option<i64>>(1)?.map(|v| v as u64),
                        status: r.get(2)?,
                    })
                })
                .map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// Process restart: in-memory jobs are gone. This is `restart_fresh`, not restore.
    pub fn reset_actual_after_process_restart(&self) -> Result<()> {
        self.write(|c| {
            c.execute(
                "UPDATE actual_state SET actual_status='stopped', last_error=NULL, updated_at=?1",
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
                g.execute_batch("COMMIT").map_err(db)?;
                Ok(v)
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
        tokio::task::spawn_blocking(f)
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

fn init(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys=ON;
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
    migrate_actual_consecutive_failures(conn)?;
    let ver = meta_u32(conn, "catalog_schema_version").unwrap_or(0);
    if ver == 0 {
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('catalog_schema_version', ?1), ('format_version', ?2)",
            params![CATALOG_SCHEMA_VERSION.to_string(), FORMAT_VERSION.to_string()],
        )
        .map_err(db)?;
    } else if ver != CATALOG_SCHEMA_VERSION {
        return Err(SparrowError::new(
            ErrorCode::FeatureUnavailable,
            format!(
                "catalog_schema_version {ver} is not supported (want {CATALOG_SCHEMA_VERSION})"
            ),
        ));
    }
    Ok(())
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
/// chars). Without those, a process-local random key is used (dev only).
/// Unprefixed plaintext is rejected (no fallback). Not a KMS. File mode 0600.
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
    if std::env::var("SPARROW_REQUIRE_SECRETS_KEY").ok().as_deref() == Some("1")
        || std::env::var("SPARROW_SAFE_MODE").ok().as_deref() == Some("1")
    {
        return Err(SparrowError::new(
            ErrorCode::SecretMissing,
            "SPARROW_SECRETS_KEY or SPARROW_SECRETS_KEY_FILE is required in safe/production mode",
        ));
    }
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

    fn spec() -> PipelineSpec {
        PipelineSpec {
            version: 1,
            stream: "sensors".into(),
            sql: Some("SELECT device_id FROM sensors".into()),
            graph: None,
            source: SourceSpec {
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
                use_demo_io: false,
                bind: None,
                path: None,
                tls: false,
            },
            sink: SinkSpec {
                kind: "http".into(),
                url: Some("http://127.0.0.1:9/ingest".into()),
                skip_verify: false,
                outbox_capacity: 8,
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
    fn put_get_and_if_match() {
        let s = Store::open_memory().unwrap();
        assert_eq!(s.schema_version().unwrap(), 1);
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
}

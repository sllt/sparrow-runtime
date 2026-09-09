//! SQLite catalog. A committed desired-state change does **not** wait for
//! MQTT/HTTP to come up — the supervisor converges afterwards.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use sparrow_model::{ErrorCode, Result, SparrowError};

use crate::spec::PipelineSpec;

pub const CATALOG_SCHEMA_VERSION: u32 = 1;
pub const FORMAT_VERSION: u32 = 1;
const AUDIT_CAP: usize = 200;
const ATTEMPT_CAP: usize = 100;

pub struct Store {
    conn: Mutex<Connection>,
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
        let conn = Connection::open(path).map_err(db)?;
        init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(db)?;
        init(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
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
            .ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, format!("unknown stream `{name}`")))
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

    pub fn put_pipeline(&self, name: &str, spec: &PipelineSpec, expected_etag: Option<&str>) -> Result<PipelineRow> {
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
                "INSERT OR IGNORE INTO actual_state(name, actual_revision, actual_status, attempt_id, last_error, updated_at)
                 VALUES (?1, NULL, 'stopped', 0, NULL, ?2)",
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

    pub fn list_pipeline_names(&self) -> Result<Vec<String>> {
        self.read(|c| {
            let mut stmt = c.prepare("SELECT name FROM pipelines ORDER BY name").map_err(db)?;
            let rows = stmt.query_map([], |r| r.get(0)).map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    pub fn set_desired(&self, name: &str, status: &str, revision: Option<u64>) -> Result<()> {
        self.write(|c| {
            let exists: i64 = c
                .query_row("SELECT COUNT(*) FROM pipelines WHERE name=?1", [name], |r| r.get(0))
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
            c.execute(
                "INSERT INTO actual_state(name, actual_revision, actual_status, attempt_id, last_error, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(name) DO UPDATE SET
                    actual_revision=excluded.actual_revision,
                    actual_status=excluded.actual_status,
                    attempt_id=excluded.attempt_id,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at",
                params![
                    name,
                    revision.map(|r| r as i64),
                    status,
                    attempt_id as i64,
                    last_error,
                    now_ms()
                ],
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
                SparrowError::new(ErrorCode::InvalidArgument, format!("no desired state for `{name}`"))
            })
        })
    }

    pub fn actual(&self, name: &str) -> Result<ActualState> {
        self.read(|c| {
            c.query_row(
                "SELECT name, actual_revision, actual_status, attempt_id, last_error FROM actual_state WHERE name=?1",
                [name],
                |r| {
                    Ok(ActualState {
                        name: r.get(0)?,
                        revision: r.get::<_, Option<i64>>(1)?.map(|v| v as u64),
                        status: r.get(2)?,
                        attempt_id: r.get::<_, i64>(3)? as u64,
                        last_error: r.get(4)?,
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

    pub fn insert_attempt(&self, pipeline: &str, revision: u64, outcome: &str, detail: Option<&str>) -> Result<i64> {
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

    pub fn audit(&self, actor: &str, action: &str, target: Option<&str>, detail: Option<&str>, outcome: &str) -> Result<()> {
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
        self.write(|c| {
            c.execute(
                "INSERT INTO secrets(name, value) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                params![name, value],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub fn get_secret(&self, name: &str) -> Result<Option<String>> {
        self.read(|c| {
            c.query_row("SELECT value FROM secrets WHERE name=?1", [name], |r| r.get(0))
                .optional()
                .map_err(db)
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
        let g = self.conn.lock().map_err(|_| {
            SparrowError::new(ErrorCode::Internal, "catalog mutex poisoned")
        })?;
        f(&g)
    }

    fn write<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        self.read(f)
    }
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
        .ok_or_else(|| SparrowError::new(ErrorCode::InvalidArgument, format!("unknown pipeline `{name}`")))?;
    let spec_json: String = c
        .query_row(
            "SELECT spec_json FROM pipeline_revisions WHERE name=?1 AND revision=?2",
            params![name, rev],
            |r| r.get(0),
        )
        .map_err(db)?;
    let spec: PipelineSpec = serde_json::from_str(&spec_json).map_err(|e| {
        SparrowError::new(ErrorCode::InvalidSchema, format!("stored spec: {e}"))
    })?;
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
            format!("catalog_schema_version {ver} is not supported (want {CATALOG_SCHEMA_VERSION})"),
        ));
    }
    Ok(())
}

fn meta_u32(conn: &Connection, key: &str) -> Result<u32> {
    let s: String = conn
        .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
        .map_err(db)?;
    s.parse().map_err(|_| SparrowError::new(ErrorCode::InvalidSchema, "meta version"))
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
            },
            sink: SinkSpec {
                kind: "http".into(),
                url: Some("http://127.0.0.1:9/ingest".into()),
                skip_verify: false,
                outbox_capacity: 8,
                use_demo_io: false,
                header_secret: None,
            },
            delivery: "live_best_effort".into(),
            recovery: "restart_fresh".into(),
            restore: None,
        }
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

//! K5.2 author-side catalog data (schema v5): drafts, connection templates and
//! publication receipts. None of these rows creates jobs, connections,
//! checkpoints, plugin workers or dependency pins; only `publish_draft`
//! touches pipelines, and it does so through the normal publication path in
//! the same transaction as the draft/receipt checks.
use super::*;

pub const MAX_DRAFTS: usize = 64;
pub const MAX_DRAFT_TEXT: usize = 64 * 1024;
pub const MAX_DRAFT_METADATA: usize = 16 * 1024;
pub const MAX_DRAFT_TOTAL: usize = 8 * 1024 * 1024;
pub const MAX_CONNECTIONS: usize = 64;
pub const MAX_CONNECTION_SPEC: usize = 16 * 1024;
pub const RECEIPT_RETENTION: usize = 512;

#[derive(Clone, Debug, serde::Serialize)]
pub struct DraftRow {
    pub id: String,
    pub etag: String,
    pub pipeline: String,
    pub mode: String,
    pub text: String,
    pub metadata: String,
    pub base_etag: Option<String>,
    pub updated_by: String,
    pub updated_at: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ConnectionRow {
    pub name: String,
    pub role: String,
    pub kind: String,
    pub version: u64,
    pub etag: String,
    pub spec_json: String,
    pub updated_by: String,
    pub updated_at: i64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct Receipt {
    pub operation_id: String,
    pub actor: String,
    pub pipeline: String,
    pub revision: u64,
    pub etag: String,
    pub draft_id: Option<String>,
    pub created_at: i64,
    /// true when this call returned an earlier committed result.
    pub replayed: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RevisionMeta {
    pub revision: u64,
    pub etag: String,
    pub created_at: i64,
    pub spec_bytes: u64,
}

pub(super) fn migrate(c: &Connection) -> Result<()> {
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS authoring_drafts(
            id TEXT PRIMARY KEY, version INTEGER NOT NULL, pipeline TEXT NOT NULL,
            mode TEXT NOT NULL, text TEXT NOT NULL, metadata TEXT NOT NULL,
            base_etag TEXT, updated_by TEXT NOT NULL, updated_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS authoring_connections(
            name TEXT PRIMARY KEY, role TEXT NOT NULL, kind TEXT NOT NULL, version INTEGER NOT NULL,
            spec_json TEXT NOT NULL, updated_by TEXT NOT NULL, updated_at INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS publication_receipts(
            operation_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL, actor TEXT NOT NULL,
            pipeline TEXT NOT NULL, revision INTEGER NOT NULL, etag TEXT NOT NULL,
            draft_id TEXT, created_at INTEGER NOT NULL);",
    )
    .map_err(db)
}

pub(super) fn schema_complete(c: &Connection) -> Result<bool> {
    let n: i64 = c
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('authoring_drafts','authoring_connections','publication_receipts')",
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    Ok(n == 3)
}

fn invalid(msg: impl Into<String>) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, msg.into())
}
pub fn conflict(msg: impl Into<String>, current: Option<String>) -> SparrowError {
    let e = SparrowError::new(ErrorCode::Conflict, msg.into());
    match current {
        Some(c) => e.context("current_etag", c),
        None => e,
    }
}
fn not_found(what: &str, id: &str) -> SparrowError {
    SparrowError::new(ErrorCode::NotFound, format!("unknown {what} `{id}`"))
}

pub fn check_operation_id(id: &str) -> Result<()> {
    if id.len() < 8 || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err(invalid("operation_id must be 8..=64 of [A-Za-z0-9_-]"));
    }
    Ok(())
}

fn draft_etag(version: u64) -> String {
    format!("draft-{version}")
}
fn parse_version(etag: &str, prefix: &str) -> Option<u64> {
    etag.trim_matches('"').strip_prefix(prefix)?.parse().ok()
}

fn load_draft(c: &Connection, id: &str) -> Result<Option<DraftRow>> {
    c.query_row(
        "SELECT id,version,pipeline,mode,text,metadata,base_etag,updated_by,updated_at,created_at FROM authoring_drafts WHERE id=?1",
        [id],
        |r| {
            Ok(DraftRow {
                id: r.get(0)?,
                etag: draft_etag(r.get::<_, i64>(1)? as u64),
                pipeline: r.get(2)?,
                mode: r.get(3)?,
                text: r.get(4)?,
                metadata: r.get(5)?,
                base_etag: r.get(6)?,
                updated_by: r.get(7)?,
                updated_at: r.get(8)?,
                created_at: r.get(9)?,
            })
        },
    )
    .optional()
    .map_err(db)
}

fn load_connection(c: &Connection, name: &str) -> Result<Option<ConnectionRow>> {
    c.query_row(
        "SELECT name,role,kind,version,spec_json,updated_by,updated_at FROM authoring_connections WHERE name=?1",
        [name],
        |r| {
            let version = r.get::<_, i64>(3)? as u64;
            Ok(ConnectionRow {
                name: r.get(0)?,
                role: r.get(1)?,
                kind: r.get(2)?,
                version,
                etag: format!("conn-{version}"),
                spec_json: r.get(4)?,
                updated_by: r.get(5)?,
                updated_at: r.get(6)?,
            })
        },
    )
    .optional()
    .map_err(db)
}

/// Input for a draft write.
pub struct DraftInput<'a> {
    pub pipeline: &'a str,
    pub mode: &'a str,
    pub text: &'a str,
    pub metadata: &'a str,
    pub base_etag: Option<&'a str>,
}

impl Store {
    pub fn list_drafts(&self) -> Result<Vec<DraftRow>> {
        self.read(|c| {
            let mut q = c.prepare("SELECT id FROM authoring_drafts ORDER BY updated_at DESC, id LIMIT 64").map_err(db)?;
            let ids: Vec<String> = q.query_map([], |r| r.get(0)).map_err(db)?.collect::<std::result::Result<_, _>>().map_err(db)?;
            ids.iter().filter_map(|id| load_draft(c, id).transpose()).collect()
        })
    }

    pub fn get_draft(&self, id: &str) -> Result<DraftRow> {
        self.read(|c| load_draft(c, id)?.ok_or_else(|| not_found("draft", id)))
    }

    /// CAS write. `expected = None` creates (fails if it exists); `Some(etag)`
    /// updates only that version. Text may be invalid; it is never bound here.
    pub fn put_draft(&self, id: &str, expected: Option<&str>, input: &DraftInput<'_>, actor: &str) -> Result<DraftRow> {
        check_name(id)?;
        check_name(input.pipeline)?;
        if !matches!(input.mode, "sql" | "graph" | "json") {
            return Err(invalid("mode must be sql, graph or json"));
        }
        if input.text.len() > MAX_DRAFT_TEXT {
            return Err(SparrowError::new(ErrorCode::ResourceExhausted, "draft text exceeds 64 KiB"));
        }
        if input.metadata.len() > MAX_DRAFT_METADATA {
            return Err(SparrowError::new(ErrorCode::ResourceExhausted, "draft metadata exceeds 16 KiB"));
        }
        if !input.metadata.is_empty() && serde_json::from_str::<serde_json::Value>(input.metadata).map(|v| !v.is_object()).unwrap_or(true) {
            return Err(invalid("draft metadata must be a JSON object"));
        }
        self.write(|c| {
            let current = load_draft(c, id)?;
            let now = now_ms();
            let version = match (&current, expected) {
                (None, None) => 1,
                (Some(cur), None) => return Err(conflict("draft already exists; send If-Match", Some(cur.etag.clone()))),
                (None, Some(_)) => return Err(conflict("draft no longer exists", None)),
                (Some(cur), Some(want)) => {
                    if want.trim_matches('"') != cur.etag {
                        return Err(conflict(format!("draft changed: If-Match `{want}` does not match `{}`", cur.etag), Some(cur.etag.clone())));
                    }
                    parse_version(&cur.etag, "draft-").unwrap_or(0) + 1
                }
            };
            if current.is_none() {
                let n: i64 = c.query_row("SELECT COUNT(*) FROM authoring_drafts", [], |r| r.get(0)).map_err(db)?;
                if n as usize >= MAX_DRAFTS {
                    return Err(SparrowError::new(ErrorCode::ResourceExhausted, "draft limit (64) reached; delete unused drafts"));
                }
            }
            let used: i64 = c
                .query_row("SELECT COALESCE(SUM(LENGTH(text)+LENGTH(metadata)),0) FROM authoring_drafts WHERE id<>?1", [id], |r| r.get(0))
                .map_err(db)?;
            if used as usize + input.text.len() + input.metadata.len() > MAX_DRAFT_TOTAL {
                return Err(SparrowError::new(ErrorCode::ResourceExhausted, "draft storage quota (8 MiB) exceeded"));
            }
            let created = current.as_ref().map(|d| d.created_at).unwrap_or(now);
            c.execute(
                "INSERT INTO authoring_drafts(id,version,pipeline,mode,text,metadata,base_etag,updated_by,updated_at,created_at)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
                 ON CONFLICT(id) DO UPDATE SET version=excluded.version,pipeline=excluded.pipeline,mode=excluded.mode,
                   text=excluded.text,metadata=excluded.metadata,base_etag=excluded.base_etag,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
                params![id, version as i64, input.pipeline, input.mode, input.text, input.metadata, input.base_etag, actor, now, created],
            )
            .map_err(db)?;
            load_draft(c, id)?.ok_or_else(|| not_found("draft", id))
        })
    }

    pub fn delete_draft(&self, id: &str, expected: &str) -> Result<()> {
        self.write(|c| {
            let cur = load_draft(c, id)?.ok_or_else(|| not_found("draft", id))?;
            if expected.trim_matches('"') != cur.etag {
                return Err(conflict("draft changed", Some(cur.etag)));
            }
            c.execute("DELETE FROM authoring_drafts WHERE id=?1", [id]).map_err(db)?;
            Ok(())
        })
    }

    /// `Some` when this operation id already committed with the same request
    /// digest; conflict when it was used for a different request.
    pub fn replay_publication(&self, operation_id: &str, request_digest: &str) -> Result<Option<Receipt>> {
        self.read(|c| match load_receipt(c, operation_id)? {
            None => Ok(None),
            Some((_, d)) if d != request_digest => Err(conflict("operation_id was already used for a different publish request", None)),
            Some((r, _)) => Ok(Some(Receipt { replayed: true, ..r })),
        })
    }

    pub fn get_receipt(&self, operation_id: &str) -> Result<Receipt> {
        self.read(|c| load_receipt(c, operation_id)?.map(|(r, _)| r).ok_or_else(|| not_found("publication", operation_id)))
    }

    /// Publish an already-validated spec from a draft. In ONE transaction:
    /// receipt idempotency (same id + digest returns the original result,
    /// different digest is rejected), draft ETag, pipeline ETag (normal
    /// publication CAS and admission), revision insert and receipt insert.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_draft(
        &self,
        operation_id: &str,
        request_digest: &str,
        actor: &str,
        draft_id: &str,
        draft_etag: &str,
        pipeline: &str,
        spec: &PipelineSpec,
        base_etag: Option<&str>,
    ) -> Result<Receipt> {
        check_operation_id(operation_id)?;
        let plugin_refs = plugin_catalog::references(spec)?;
        self.write(|c| {
            if let Some((r, digest)) = load_receipt(c, operation_id)? {
                if digest != request_digest {
                    return Err(conflict("operation_id was already used for a different publish request", None));
                }
                return Ok(Receipt { replayed: true, ..r });
            }
            let draft = load_draft(c, draft_id)?.ok_or_else(|| not_found("draft", draft_id))?;
            if draft.etag != draft_etag.trim_matches('"') {
                return Err(conflict("draft changed since review; reload and review again", Some(draft.etag)));
            }
            if draft.pipeline != pipeline {
                return Err(invalid("draft targets a different pipeline"));
            }
            let current: Option<String> = c.query_row("SELECT etag FROM pipelines WHERE name=?1", [pipeline], |r| r.get(0)).optional().map_err(db)?;
            match (&current, base_etag) {
                (Some(cur), None) => return Err(conflict("pipeline already exists; base ETag required", Some(cur.clone()))),
                (Some(cur), Some(want)) if want.trim_matches('"') != cur => {
                    return Err(conflict(format!("pipeline changed: base `{want}` is not current `{cur}`"), Some(cur.clone())))
                }
                (None, Some(_)) => return Err(conflict("pipeline was deleted since review", None)),
                _ => {}
            }
            let row = self.put_pipeline_in(c, pipeline, spec, current.as_deref(), false, None, &plugin_refs)?;
            let now = now_ms();
            c.execute(
                "INSERT INTO publication_receipts(operation_id,request_digest,actor,pipeline,revision,etag,draft_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![operation_id, request_digest, actor, pipeline, row.latest_revision as i64, row.etag, draft_id, now],
            )
            .map_err(db)?;
            c.execute(
                "DELETE FROM publication_receipts WHERE operation_id IN (SELECT operation_id FROM publication_receipts ORDER BY created_at DESC, rowid DESC LIMIT -1 OFFSET ?1)",
                [RECEIPT_RETENTION as i64],
            )
            .map_err(db)?;
            // The draft now describes the published revision; rebase it so the
            // next edit is checked against the new pipeline ETag.
            c.execute("UPDATE authoring_drafts SET base_etag=?2, version=version+1, updated_at=?3, updated_by=?4 WHERE id=?1", params![draft_id, row.etag, now, actor])
                .map_err(db)?;
            Ok(Receipt {
                operation_id: operation_id.into(),
                actor: actor.into(),
                pipeline: pipeline.into(),
                revision: row.latest_revision,
                etag: row.etag,
                draft_id: Some(draft_id.into()),
                created_at: now,
                replayed: false,
            })
        })
    }

    pub fn list_pipeline_revisions(&self, name: &str, before: Option<u64>, limit: usize) -> Result<Vec<RevisionMeta>> {
        let limit = limit.clamp(1, 100) as i64;
        self.read(|c| {
            let mut q = c
                .prepare("SELECT revision, etag, created_at, LENGTH(spec_json) FROM pipeline_revisions WHERE name=?1 AND revision<?2 ORDER BY revision DESC LIMIT ?3")
                .map_err(db)?;
            let rows = q
                .query_map(params![name, before.map(|b| b.min(i64::MAX as u64) as i64).unwrap_or(i64::MAX), limit], |r| {
                    Ok(RevisionMeta { revision: r.get::<_, i64>(0)? as u64, etag: r.get(1)?, created_at: r.get(2)?, spec_bytes: r.get::<_, i64>(3)? as u64 })
                })
                .map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    pub fn list_secret_names(&self) -> Result<Vec<String>> {
        self.read(|c| {
            let mut q = c.prepare("SELECT name FROM secrets ORDER BY name LIMIT 1024").map_err(db)?;
            let rows = q.query_map([], |r| r.get(0)).map_err(db)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// Stream ETag: digest of the stored schema text.
    pub fn stream_etag(schema_json: &str) -> String {
        let d = ring::digest::digest(&ring::digest::SHA256, schema_json.as_bytes());
        format!("schema-{}", d.as_ref()[..8].iter().map(|b| format!("{b:02x}")).collect::<String>())
    }

    /// Conditional stream write: `expected = "absent"` creates only; otherwise
    /// must equal the current `stream_etag`. Check and write share a transaction.
    pub fn put_stream_if(&self, name: &str, schema_json: &str, expected: &str) -> Result<String> {
        check_name(name)?;
        self.write(|c| {
            let cur: Option<String> = c.query_row("SELECT schema_json FROM streams WHERE name=?1", [name], |r| r.get(0)).optional().map_err(db)?;
            let cur_tag = cur.as_deref().map(Self::stream_etag);
            let want = expected.trim_matches('"');
            let ok = match &cur_tag { None => want == "absent", Some(t) => want == t };
            if !ok {
                return Err(conflict(format!("stream changed: If-Match `{want}` does not match"), Some(cur_tag.unwrap_or_else(|| "absent".into()))));
            }
            c.execute(
                "INSERT INTO streams(name, schema_json, created_at) VALUES (?1, ?2, ?3) ON CONFLICT(name) DO UPDATE SET schema_json=excluded.schema_json",
                params![name, schema_json, now_ms()],
            )
            .map_err(db)?;
            self.inner.stream_epoch.fetch_add(1, Ordering::Relaxed);
            Ok(Self::stream_etag(schema_json))
        })
    }

    pub fn list_connections(&self) -> Result<Vec<ConnectionRow>> {
        self.read(|c| {
            let mut q = c.prepare("SELECT name FROM authoring_connections ORDER BY name LIMIT 64").map_err(db)?;
            let names: Vec<String> = q.query_map([], |r| r.get(0)).map_err(db)?.collect::<std::result::Result<_, _>>().map_err(db)?;
            names.iter().filter_map(|n| load_connection(c, n).transpose()).collect()
        })
    }

    pub fn get_connection(&self, name: &str) -> Result<ConnectionRow> {
        self.read(|c| load_connection(c, name)?.ok_or_else(|| not_found("connection", name)))
    }

    /// Template CAS write. Templates are author-side only: published pipelines
    /// hold expanded specs and are never rewritten by a template update.
    pub fn put_connection(&self, name: &str, expected: Option<&str>, role: &str, kind: &str, spec_json: &str, actor: &str) -> Result<ConnectionRow> {
        check_name(name)?;
        if !matches!(role, "source" | "sink") {
            return Err(invalid("role must be source or sink"));
        }
        if spec_json.len() > MAX_CONNECTION_SPEC {
            return Err(SparrowError::new(ErrorCode::ResourceExhausted, "connection template exceeds 16 KiB"));
        }
        self.write(|c| {
            let cur = load_connection(c, name)?;
            let version = match (&cur, expected) {
                (None, None) => 1,
                (Some(x), None) => return Err(conflict("connection exists; send If-Match", Some(x.etag.clone()))),
                (None, Some(_)) => return Err(conflict("connection no longer exists", None)),
                (Some(x), Some(w)) if w.trim_matches('"') != x.etag => return Err(conflict("connection changed", Some(x.etag.clone()))),
                (Some(x), Some(_)) => x.version + 1,
            };
            if cur.is_none() {
                let n: i64 = c.query_row("SELECT COUNT(*) FROM authoring_connections", [], |r| r.get(0)).map_err(db)?;
                if n as usize >= MAX_CONNECTIONS {
                    return Err(SparrowError::new(ErrorCode::ResourceExhausted, "connection template limit (64) reached"));
                }
            }
            c.execute(
                "INSERT INTO authoring_connections(name,role,kind,version,spec_json,updated_by,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7)
                 ON CONFLICT(name) DO UPDATE SET role=excluded.role,kind=excluded.kind,version=excluded.version,spec_json=excluded.spec_json,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
                params![name, role, kind, version as i64, spec_json, actor, now_ms()],
            )
            .map_err(db)?;
            load_connection(c, name)?.ok_or_else(|| not_found("connection", name))
        })
    }

    pub fn delete_connection(&self, name: &str, expected: &str) -> Result<()> {
        self.write(|c| {
            let cur = load_connection(c, name)?.ok_or_else(|| not_found("connection", name))?;
            if expected.trim_matches('"') != cur.etag {
                return Err(conflict("connection changed", Some(cur.etag)));
            }
            c.execute("DELETE FROM authoring_connections WHERE name=?1", [name]).map_err(db)?;
            Ok(())
        })
    }
}

fn load_receipt(c: &Connection, id: &str) -> Result<Option<(Receipt, String)>> {
    c.query_row(
        "SELECT operation_id,actor,pipeline,revision,etag,draft_id,created_at,request_digest FROM publication_receipts WHERE operation_id=?1",
        [id],
        |r| {
            Ok((
                Receipt {
                    operation_id: r.get(0)?,
                    actor: r.get(1)?,
                    pipeline: r.get(2)?,
                    revision: r.get::<_, i64>(3)? as u64,
                    etag: r.get(4)?,
                    draft_id: r.get(5)?,
                    created_at: r.get(6)?,
                    replayed: false,
                },
                r.get(7)?,
            ))
        },
    )
    .optional()
    .map_err(db)
}

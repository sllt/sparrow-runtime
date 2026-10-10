use super::*;
use crate::input_dlq::invalid;
use serde_json::{json, Value};

fn key(operation: &str) -> String {
    format!("recovery_operation:{operation}")
}
fn load(c: &Connection, operation: &str) -> Result<Option<Value>> {
    let raw: Option<String> = c
        .query_row(
            "SELECT value FROM meta WHERE key=?1",
            [key(operation)],
            |r| r.get(0),
        )
        .optional()
        .map_err(db)?;
    raw.map(|s| {
        serde_json::from_str(&s).map_err(|_| invalid("invalid persisted recovery operation"))
    })
    .transpose()
}
fn save(c: &Connection, id: &str, value: &Value) -> Result<()> {
    let mut value = value.clone();
    value["updated_at_ms"] = json!(now_ms());
    let encoded =
        serde_json::to_string(&value).map_err(|_| invalid("recovery operation encoding"))?;
    if encoded.len() > 128 * 1024 {
        return Err(invalid("recovery metadata exceeds 128KiB"));
    }
    c.execute("INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key(id),encoded]).map_err(db)?;
    Ok(())
}
impl Store {
    pub fn recovery_operation(&self, id: &str) -> Result<Option<Value>> {
        check_name(id)?;
        self.read(|c| load(c, id))
    }
    pub fn recovery_operations(&self, parent: &str) -> Result<Value> {
        self.read(|c| {
            let mut q=c.prepare("SELECT value FROM meta WHERE key LIKE 'recovery_operation:%' ORDER BY key LIMIT 128").map_err(db)?;
            let mut rows=Vec::new();for raw in q.query_map([],|r|r.get::<_,String>(0)).map_err(db)? {
                let v:Value=serde_json::from_str(&raw.map_err(db)?).map_err(|_|invalid("recovery metadata invalid"))?;
                if v["parent"]==parent {rows.push(json!({"operation":v["operation"],"target":v["target"],"mode":v["mode"],"phase":v["phase"],"attempts":v["attempts"],"published_revision":v["published_revision"],"last_error_code":v["last_error_code"]}));}
            }Ok(json!({"operations":rows}))
        })
    }
    pub(crate) fn reserve_recovery(&self, p: &crate::recovery_ops::Prepared) -> Result<()> {
        self.write(|c| {
            let id=&p.request.operation;
            if let Some(mut old)=load(c,id)? {
                if old["request_hash"]!=p.request_hash || old["parent"]!=p.parent || old["phase"]!="preparing" {return Err(invalid("operation ID is already bound to another request or is finished"));}
                let n=old["attempts"].as_u64().unwrap_or(100);
                if n>=16 {return Err(invalid("recovery operation attempt budget exhausted"));}
                old["attempts"]=json!(n+1);return save(c,id,&old);
            }
            let n:u64=c.query_row("SELECT count(*) FROM meta WHERE key LIKE 'recovery_operation:%'",[],|r|r.get(0)).map_err(db)?;
            if n>=128 {return Err(invalid("recovery operation catalog limit (128) reached"));}
            let lock=format!("recovery_target:{}",p.request.target);
            let previous:Option<String>=c.query_row("SELECT value FROM meta WHERE key=?1",[&lock],|r|r.get(0)).optional().map_err(db)?;
            if let Some(previous)=previous {
                let old=load(c,&previous)?.ok_or_else(||invalid("old target reservation missing"))?;
                if old["phase"]=="preparing" || p.request.mode!="resume" {return Err(invalid("target already belongs to another recovery operation"));}
            }
            let exists:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM pipelines WHERE name=?1)",[&p.request.target],|r|r.get(0)).map_err(db)?;
            if exists && p.request.mode!="resume" {return Err(invalid("new lineage target already exists"));}
            let mut value=p.view();
            value["phase"]=json!("preparing");value["request_hash"]=json!(p.request_hash);value["schema"]=json!(p.schema);value["parent_schema"]=json!(p.parent_schema);value["attempts"]=json!(1);
            value["created_at_ms"]=json!(now_ms());
            value["reason"]=json!(p.request.reason);value["artifact_sha256"]=json!(p.artifact.as_ref().map(|b|crate::input_dlq::hash(b)));
            value["artifact_directory"]=json!(p.request.artifact_directory);
            save(c,id,&value)?;
            c.execute("INSERT INTO meta(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![lock,id]).map_err(db)?;Ok(())
        })
    }
    pub(crate) fn publish_recovery(
        &self,
        p: &crate::recovery_ops::Prepared,
    ) -> Result<PipelineRow> {
        let etag = format!("rev-{}", p.request.approve_parent_revision);
        self.put_pipeline_with_activation(
            &p.request.target,
            &p.spec,
            if p.request.mode == "resume" {
                Some(&etag)
            } else {
                None
            },
            false,
            Some(&p.request.operation),
        )
    }
    pub(crate) fn recovery_error(&self, id: &str, code: ErrorCode) -> Result<()> {
        self.write(|c| {
            let mut v = load(c, id)?.ok_or_else(|| invalid("unknown operation"))?;
            if v["phase"] == "preparing" {
                v["last_error_code"] = json!(code.as_str());
                save(c, id, &v)?;
            }
            Ok(())
        })
    }
    pub(crate) fn finish_recovery(&self, parent: &str, id: &str, checkpoint: u64) -> Result<Value> {
        self.write(|c| {
            let mut v = load(c, id)?.ok_or_else(|| invalid("operation missing"))?;
            if v["parent"] != parent || v["phase"] != "ready" {
                return Err(invalid(
                    "operation does not belong to this parent or is not ready",
                ));
            }
            let target = v["target"]
                .as_str()
                .ok_or_else(|| invalid("operation target missing"))?
                .to_string();
            c.execute(
                "UPDATE desired_state SET desired_status='stopped',updated_at=?2 WHERE name=?1",
                params![target, now_ms()],
            )
            .map_err(db)?;
            v["finished_checkpoint"] = json!(checkpoint);
            save(c, id, &v)?;
            Ok(v)
        })
    }
    pub fn abort_recovery(&self, parent: &str, id: &str, reason: &str) -> Result<Value> {
        crate::input_dlq::audit_reason(reason)?;
        self.write(|c| {
            let mut v = load(c, id)?.ok_or_else(|| invalid("operation missing"))?;
            if v["parent"] != parent || v["phase"] != "preparing" {
                return Err(invalid("only a pending operation can be aborted"));
            }
            let target = v["target"]
                .as_str()
                .ok_or_else(|| invalid("operation target missing"))?;
            c.execute(
                "DELETE FROM meta WHERE key=?1 AND value=?2",
                params![format!("recovery_target:{target}"), id],
            )
            .map_err(db)?;
            v["phase"] = json!("aborted");
            v["abort_reason"] = json!(reason);
            save(c, id, &v)?;
            Ok(v)
        })
    }
}
pub(super) fn publication_guard(
    c: &Connection,
    name: &str,
    spec: &PipelineSpec,
    operation: Option<&str>,
) -> Result<()> {
    check_dependency_directory(c, name, spec)?;
    let reserved: Option<String> = c
        .query_row(
            "SELECT value FROM meta WHERE key=?1",
            [format!("recovery_target:{name}")],
            |r| r.get(0),
        )
        .optional()
        .map_err(db)?;
    if let Some(id) = operation {
        if reserved.as_deref() != Some(id) {
            return Err(invalid("recovery target reservation mismatch"));
        }
        let value = load(c, id)?.ok_or_else(|| invalid("recovery operation missing"))?;
        if value["phase"] != "preparing" || value["target"] != name {
            return Err(invalid("recovery operation is not publishable"));
        }
        let parent = value["parent"]
            .as_str()
            .ok_or_else(|| invalid("operation parent missing"))?;
        let row = load_pipeline(c, parent)?;
        let parent_schema: String = c
            .query_row(
                "SELECT schema_json FROM streams WHERE name=?1",
                [&row.spec.stream],
                |r| r.get(0),
            )
            .map_err(db)?;
        if value["parent_schema"] != parent_schema {
            return Err(invalid("parent schema changed during recovery operation"));
        }
        if value["parent_revision"] != row.latest_revision {
            return Err(invalid("parent changed during recovery operation"));
        }
        let desired: String = c
            .query_row(
                "SELECT desired_status FROM desired_state WHERE name=?1",
                [parent],
                |r| r.get(0),
            )
            .map_err(db)?;
        let actual: String = c
            .query_row(
                "SELECT actual_status FROM actual_state WHERE name=?1",
                [parent],
                |r| r.get(0),
            )
            .map_err(db)?;
        if desired != "stopped" || actual != "stopped" {
            return Err(invalid("parent started during recovery operation"));
        }
        let schema: String = c
            .query_row(
                "SELECT schema_json FROM streams WHERE name=?1",
                [&spec.stream],
                |r| r.get(0),
            )
            .map_err(db)?;
        if value["schema"] != schema {
            return Err(invalid("target schema changed during recovery operation"));
        }
    } else if let Some(id) = reserved {
        let value = load(c, &id)?.ok_or_else(|| invalid("recovery reservation is corrupt"))?;
        if value["phase"] == "preparing" {
            return Err(invalid(
                "target is reserved by a pending recovery operation",
            ));
        }
        if let Some(start) = &spec.source.replay_start {
            let lineage = load(c, &start.operation)?.ok_or_else(|| invalid("lineage missing"))?;
            if lineage["target"] != name || lineage["phase"] != "ready" {
                return Err(invalid("lineage target differs"));
            }
        }
    } else if spec.source.replay_start.is_some() {
        return Err(invalid(
            "replay_start can only be created by an approved recovery operation",
        ));
    }
    Ok(())
}

fn check_dependency_directory(c: &Connection, name: &str, spec: &PipelineSpec) -> Result<()> {
    fn enhanced(s: &PipelineSpec) -> bool {
        s.source.input_dlq.is_some()
            || s.source.replay_start.is_some()
            || s.sink.durable_outbox.is_some()
    }
    fn directory(s: &PipelineSpec) -> Option<String> {
        if s.recovery != "aligned" {
            return None;
        }
        s.checkpoint_dir
            .clone()
            .or_else(|| s.source.path.as_ref().map(|p| format!("{p}.sparrow-chk")))
    }
    let Some(dir) = directory(spec) else {
        return Ok(());
    };
    let mut q = c
        .prepare("SELECT spec_json FROM pipeline_revisions WHERE name<>?1")
        .map_err(db)?;
    for raw in q.query_map([name], |r| r.get::<_, String>(0)).map_err(db)? {
        let other: PipelineSpec = serde_json::from_str(&raw.map_err(db)?)
            .map_err(|_| invalid("invalid retained dependency spec"))?;
        if !enhanced(spec) && !enhanced(&other) {
            continue;
        }
        let live = sparrow_connectors::policy::checked_data_path_identity(Path::new(&dir))
            .map_err(SparrowError::from)?;
        if let Some(dir) = directory(&other) {
            let old = sparrow_connectors::policy::checked_data_path_identity(Path::new(&dir))
                .map_err(SparrowError::from)?;
            if old.starts_with(&live) || live.starts_with(&old) {
                return Err(invalid("checkpoint directory belongs to another durable dependency/lineage; use a new directory"));
            }
        }
    }
    Ok(())
}
pub(super) fn publication_finished(
    c: &Connection,
    id: &str,
    revision: u64,
    spec: &PipelineSpec,
) -> Result<()> {
    let mut value = load(c, id)?.ok_or_else(|| invalid("recovery operation missing"))?;
    value["phase"] = json!("ready");
    value["published_revision"] = json!(revision);
    value["last_error_code"] = Value::Null;
    value["target_spec"] =
        serde_json::to_value(spec).map_err(|_| invalid("lineage spec encoding"))?;
    save(c, id, &value)
}

//! Durable plugin references are catalog-owned, not transient expression pins.
//! All managed uninstall/publication paths acquire catalog -> registry in that
//! order, and use a SQLite write transaction against other catalog connections.
use super::*;
use sparrow_expr::plugins::PackageReference;

pub(crate) fn references(spec: &PipelineSpec) -> Result<Vec<PackageReference>> {
    // Cheap absence check only; actual references always come from the AST.
    // Historical invalid non-plugin drafts must not block this migration.
    let mut refs = if let Some(sql) = spec
        .sql
        .as_ref()
        .filter(|s| s.to_ascii_lowercase().contains("plugin_call"))
    {
        sparrow_sql::plugin_references::extract(sql)?
    } else {
        vec![]
    };
    fn graph(value: &serde_json::Value, refs: &mut Vec<PackageReference>) -> Result<()> {
        match value {
            serde_json::Value::Object(map) => {
                if map.get("k").and_then(|v| v.as_str()) == Some("call")
                    && map
                        .get("name")
                        .and_then(|v| v.as_str())
                        .is_some_and(|n| n.eq_ignore_ascii_case("plugin_call"))
                {
                    let args = map
                        .get("args")
                        .and_then(|v| v.as_array())
                        .filter(|a| (4..=12).contains(&a.len()))
                        .ok_or_else(|| plugin_error("invalid Graph plugin dependency"))?;
                    let names: Option<Vec<_>> = args[..4]
                        .iter()
                        .map(|v| {
                            (v["k"] == "lit" && v["value"]["t"] == "utf8")
                                .then(|| v["value"]["v"].as_str())
                                .flatten()
                        })
                        .collect();
                    let names = names
                        .ok_or_else(|| plugin_error("Graph plugin references must be literal"))?;
                    let reference = PackageReference {
                        name: names[0].into(),
                        version: names[1].into(),
                        manifest_sha256: names[2].into(),
                    };
                    reference.validate()?;
                    if !sparrow_expr::plugins::identifier(names[3]) {
                        return Err(plugin_error("invalid plugin function"));
                    }
                    if !refs.contains(&reference) {
                        refs.push(reference);
                    }
                }
                for value in map.values() {
                    graph(value, refs)?;
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    graph(value, refs)?;
                }
            }
            _ => {}
        }
        if refs.len() > 64 {
            return Err(plugin_error("too many pipeline plugin dependencies"));
        }
        Ok(())
    }
    if let Some(spec) = &spec.graph {
        for node in &spec.nodes {
            // Only expression trees are code. In particular plugin.config is
            // opaque data and may legally contain objects shaped like calls.
            let expressions=node.predicate.iter()
                .chain(node.exprs.iter().flatten().map(|e|&e.expr))
                .chain(node.aggs.iter().flatten().filter_map(|e|e.expr.as_ref()))
                .chain(node.routes.iter().flatten().map(|e|&e.predicate))
                .chain(node.unnest.iter().map(|e|&e.expr));
            for expr in expressions {
                graph(&serde_json::to_value(expr).map_err(|_|plugin_error("invalid Graph expression"))?, &mut refs)?;
            }
            if let Some(binding)=&node.plugin {binding.validate()?;refs.push(binding.reference());}
        }
    }
    for binding in spec.source.plugin.iter().chain(spec.sink.plugin.iter())
        .chain(spec.graph_io.iter().flat_map(|io|io.sources.values().filter_map(|s|s.plugin.as_ref())))
        .chain(spec.graph_io.iter().flat_map(|io|io.sinks.values().filter_map(|s|s.plugin.as_ref()))) {
        binding.validate()?;refs.push(binding.reference());
    }
    refs.sort_by(|a,b|a.manifest_sha256.cmp(&b.manifest_sha256));
    refs.dedup();
    if refs.len()>64 {return Err(plugin_error("too many pipeline plugin dependencies"));}
    Ok(refs)
}
fn plugin_error(message: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, message)
}
pub(super) fn insert(
    c: &Connection,
    name: &str,
    revision: u64,
    refs: &[PackageReference],
) -> Result<()> {
    for reference in refs {
        c.execute("INSERT INTO pipeline_plugin_dependencies(pipeline_name,pipeline_revision,package_name,package_version,manifest_sha256) VALUES(?1,?2,?3,?4,?5)",
            params![name,revision as i64,reference.name,reference.version,reference.manifest_sha256]).map_err(db)?;
    }
    Ok(())
}
pub(super) fn schema_complete(c: &Connection) -> Result<bool> {
    let table:i64=c.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='pipeline_plugin_dependencies'",[],|r|r.get(0)).map_err(db)?;
    if table != 1 {
        return Ok(false);
    }
    let mut stmt = c
        .prepare("PRAGMA table_info(pipeline_plugin_dependencies)")
        .map_err(db)?;
    let columns = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .map_err(db)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(db)?;
    let expected = [
        ("pipeline_name", "TEXT", 1),
        ("pipeline_revision", "INTEGER", 2),
        ("package_name", "TEXT", 0),
        ("package_version", "TEXT", 0),
        ("manifest_sha256", "TEXT", 3),
    ]
    .into_iter()
    .map(|(name, ty, pk)| (name.to_owned(), ty.to_owned(), 1, pk))
    .collect::<Vec<_>>();
    if columns != expected {
        return Ok(false);
    }
    let mut stmt = c
        .prepare("PRAGMA foreign_key_list(pipeline_plugin_dependencies)")
        .map_err(db)?;
    let keys = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(6)?,
            ))
        })
        .map_err(db)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(db)?;
    Ok(keys
        == [("pipeline_name", "name"), ("pipeline_revision", "revision")]
            .into_iter()
            .map(|(from, to)| {
                (
                    "pipeline_revisions".into(),
                    from.into(),
                    to.into(),
                    "RESTRICT".into(),
                )
            })
            .collect::<Vec<_>>())
}
pub(super) fn migrate(c: &Connection) -> Result<()> {
    c.execute_batch("CREATE TABLE pipeline_plugin_dependencies(
        pipeline_name TEXT NOT NULL,pipeline_revision INTEGER NOT NULL,
        package_name TEXT NOT NULL,package_version TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,
        PRIMARY KEY(pipeline_name,pipeline_revision,manifest_sha256),
        FOREIGN KEY(pipeline_name,pipeline_revision) REFERENCES pipeline_revisions(name,revision) ON DELETE RESTRICT);
        CREATE INDEX pipeline_plugin_digest ON pipeline_plugin_dependencies(manifest_sha256);").map_err(db)?;
    // Stream history; don't materialize an unbounded collection of SQL/specs.
    let mut stmt=c.prepare("SELECT name,revision,length(CAST(spec_json AS BLOB)),spec_json FROM pipeline_revisions ORDER BY name,revision").map_err(db)?;
    let mut rows = stmt.query([]).map_err(db)?;
    while let Some(row) = rows.next().map_err(db)? {
        let size: i64 = row.get(2).map_err(db)?;
        if size < 0 || size as usize > crate::spec::MAX_SPEC_BYTES {
            return Err(plugin_error(
                "historical pipeline spec exceeds migration bound",
            ));
        }
        let name: String = row.get(0).map_err(db)?;
        let revision: i64 = row.get(1).map_err(db)?;
        let raw: String = row.get(3).map_err(db)?;
        if revision < 1 {
            return Err(plugin_error("invalid historical pipeline revision"));
        }
        let spec: PipelineSpec = serde_json::from_str(&raw)
            .map_err(|_| plugin_error("invalid historical pipeline spec"))?;
        insert(c, &name, revision as u64, &references(&spec)?)?;
    }
    Ok(())
}
impl Store {
    pub fn plugin_references(&self, digest: &str) -> Result<serde_json::Value> {
        if !sparrow_expr::plugins::digest_name(digest) {
            return Err(plugin_error("invalid manifest digest"));
        }
        self.read(|c| {
            let count:i64=c.query_row("SELECT COUNT(*) FROM pipeline_plugin_dependencies WHERE manifest_sha256=?1",[digest],|r|r.get(0)).map_err(db)?;
            let mut stmt=c.prepare("SELECT pipeline_name,pipeline_revision FROM pipeline_plugin_dependencies WHERE manifest_sha256=?1 ORDER BY pipeline_name,pipeline_revision LIMIT 64").map_err(db)?;
            let rows=stmt.query_map([digest],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).map_err(db)?;
            let refs=rows.collect::<std::result::Result<Vec<_>,_>>().map_err(db)?.into_iter().map(|(name,revision)|serde_json::json!({"pipeline":name,"revision":revision})).collect::<Vec<_>>();
            Ok(serde_json::json!({"manifest_sha256":digest,"count":count,"references":refs,"truncated":count>64}))
        })
    }
    pub fn uninstall_plugin(&self, digest: &str) -> Result<()> {
        let manager = crate::plugins::manager(self)?;
        self.write(|c| {
            let count:i64=c.query_row("SELECT COUNT(*) FROM pipeline_plugin_dependencies WHERE manifest_sha256=?1",[digest],|r|r.get(0)).map_err(db)?;
            if count!=0 {return Err(SparrowError::new(ErrorCode::ResourceExhausted,"plugin is referenced by retained pipeline revisions; retire stopped fresh pipelines before uninstall").context("pins",count.to_string()));}
            manager.uninstall(digest)
        })
    }
    /// Explicit destructive catalog maintenance; never removes external files.
    pub fn retire_pipeline(&self, name: &str, approve_etag: &str) -> Result<()> {
        check_name(name)?;
        self.write(|c| {
            let etag:String=c.query_row("SELECT etag FROM pipelines WHERE name=?1",[name],|r|r.get(0)).map_err(db)?;
            if etag!=approve_etag {return Err(plugin_error("retirement requires exact current pipeline ETag approval"));}
            for (table,column) in [("desired_state","desired_status"),("actual_state","actual_status")] {
                let status:Option<String>=c.query_row(&format!("SELECT {column} FROM {table} WHERE name=?1"),[name],|r|r.get(0)).optional().map_err(db)?;
                if status.as_deref().is_some_and(|s|s!="stopped") {return Err(plugin_error("pipeline must be desired and observed stopped before retirement"));}
            }
            let mut stmt=c.prepare("SELECT length(CAST(spec_json AS BLOB)),spec_json FROM pipeline_revisions WHERE name=?1").map_err(db)?;
            let mut rows=stmt.query([name]).map_err(db)?;
            while let Some(row)=rows.next().map_err(db)? {
                let size:i64=row.get(0).map_err(db)?;
                if size<0||size as usize>crate::spec::MAX_SPEC_BYTES {return Err(plugin_error("retained spec exceeds retirement bound"));}
                let raw:String=row.get(1).map_err(db)?;
                let spec:PipelineSpec=serde_json::from_str(&raw).map_err(|_|plugin_error("invalid retained spec"))?;
                if spec.recovery!="restart_fresh"||spec.restore.is_some()||spec.checkpoint.is_some()||spec.checkpoint_dir.is_some() {
                    return Err(plugin_error("retirement refuses checkpoint-bearing history; use separate checkpoint lifecycle maintenance"));
                }
            }
            drop(rows);drop(stmt);
            c.execute("DELETE FROM pipeline_plugin_dependencies WHERE pipeline_name=?1",[name]).map_err(db)?;
            c.execute("DELETE FROM pipeline_reference_tables WHERE pipeline_name=?1",[name]).map_err(db)?;
            c.execute("DELETE FROM pipeline_revisions WHERE name=?1",[name]).map_err(db)?;
            c.execute("DELETE FROM pipelines WHERE name=?1",[name]).map_err(db)?;
            c.execute("DELETE FROM desired_state WHERE name=?1",[name]).map_err(db)?;
            c.execute("DELETE FROM actual_state WHERE name=?1",[name]).map_err(db)?;
            c.execute("DELETE FROM deployment_attempts WHERE pipeline=?1",[name]).map_err(db)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_expr::plugins::{sha256, Manager, Manifest};
    fn package() -> (std::path::PathBuf, Arc<Manager>, String) {
        let path = std::env::temp_dir().join(format!(
            "sparrow-ref-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        let manager = Manager::open(&path.join("packages"), false).unwrap();
        let source = b"({f(v){return v;}})";
        let manifest:Manifest=serde_json::from_value(serde_json::json!({"format":1,"name":"pkg","version":"v1","kind":"javascript_scalar","abi":1,"semantics":1,"target":sparrow_expr::plugins::JS_TARGET,"artifact_sha256":sha256(source),"deterministic":true,"thread_safe":true,"null_policy":"propagate","functions":[{"name":"f","id":1,"inputs":["int64"],"output":"int64","max_output_bytes":8}]})).unwrap();
        let hash = manager.install(manifest, source).unwrap().manifest_sha256;
        (path, manager, hash)
    }
    fn spec(hash: &str) -> PipelineSpec {
        serde_json::from_value(serde_json::json!({"version":1,"stream":"s","sql":format!("SELECT plugin_call('pkg','v1','{hash}','f',value) AS value FROM s"),"source":{"kind":"file","path":"/unused"},"sink":{"kind":"log"},"recovery":"restart_fresh"})).unwrap()
    }
    #[test]
    fn packages_catalog_references_atomic_history_retirement_and_uninstall() {
        let (path, manager, hash) = package();
        let store = Store::open_memory().unwrap();
        store.configure_plugins(manager.clone()).unwrap();
        let mut spec = spec(&hash);
        store.put_pipeline("p", &spec, None).unwrap();
        assert_eq!(store.plugin_references(&hash).unwrap()["count"], 1);
        store.inner.fail_before_commit.store(true, Ordering::SeqCst);
        assert!(store.put_pipeline("p", &spec, Some("rev-1")).is_err());
        store
            .inner
            .fail_before_commit
            .store(false, Ordering::SeqCst);
        assert_eq!(store.plugin_references(&hash).unwrap()["count"], 1);
        spec.sql = Some("SELECT value FROM s".into());
        store.put_pipeline("p", &spec, Some("rev-1")).unwrap();
        assert!(store.uninstall_plugin(&hash).is_err());
        assert!(store.retire_pipeline("p", "rev-1").is_err());
        store
            .write(|c| {
                c.execute(
                    "UPDATE desired_state SET desired_status='running' WHERE name='p'",
                    [],
                )
                .map_err(db)?;
                Ok(())
            })
            .unwrap();
        assert!(store.retire_pipeline("p", "rev-2").is_err());
        store
            .write(|c| {
                c.execute(
                    "UPDATE desired_state SET desired_status='stopped' WHERE name='p'",
                    [],
                )
                .map_err(db)?;
                Ok(())
            })
            .unwrap();
        store.retire_pipeline("p", "rev-2").unwrap();
        assert_eq!(store.plugin_references(&hash).unwrap()["count"], 0);
        store.uninstall_plugin(&hash).unwrap();
        drop(store);
        drop(manager);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn packages_catalog_v3_backfill_and_v4_missing_table_fail_closed() {
        let (path, manager, hash) = package();
        let dbfile = path.join("catalog.db");
        let store = Store::open(&dbfile).unwrap();
        store.configure_plugins(manager.clone()).unwrap();
        store.put_pipeline("p", &spec(&hash), None).unwrap();
        store.write(|c|{c.execute_batch("DROP TABLE pipeline_plugin_dependencies; UPDATE meta SET value='3' WHERE key='catalog_schema_version';").map_err(db)}).unwrap();
        drop(store);
        let store = Store::open(&dbfile).unwrap();
        assert_eq!(store.schema_version().unwrap(), 4);
        assert_eq!(store.plugin_references(&hash).unwrap()["count"], 1);
        store
            .write(|c| {
                c.execute_batch("DROP TABLE pipeline_plugin_dependencies;")
                    .map_err(db)
            })
            .unwrap();
        drop(store);
        assert!(Store::open(&dbfile).is_err());
        drop(manager);
        std::fs::remove_dir_all(path).unwrap();
    }
}

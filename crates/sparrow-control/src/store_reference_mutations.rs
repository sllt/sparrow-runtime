//! Incremental reference-table writes, always inside one SQLite transaction.
use super::*;
use crate::reference_table::{MutationSpec, RollbackSpec};

impl Store {
    /// Poll a hot follower without materializing unchanged row JSON.  Head
    /// metadata and changed contents share a SQLite read snapshot, so GC
    /// through another Store/connection cannot delete the selected revision
    /// between reading the head and checking its canonical content hash.
    pub fn reference_update_after(
        &self,
        name: &str,
        after: u64,
    ) -> Result<Option<ReferenceTableRow>> {
        check_name(name)?;
        if after > i64::MAX as u64 {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "reference table follower revision exceeds SQLite integer range",
            ));
        }
        self.read(|c| {
            c.execute_batch("BEGIN DEFERRED").map_err(db)?;
            let result = (|| {
                let revision: Option<i64> = c
                    .query_row(
                        "SELECT latest_revision FROM reference_table_heads WHERE name=?1",
                        [name],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(db)?;
                let revision = match revision {
                    Some(revision) if revision > 0 => revision as u64,
                    Some(_) => {
                        return Err(SparrowError::new(
                            ErrorCode::CodecViolation,
                            "reference table head revision is invalid",
                        ));
                    }
                    None => {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidArgument,
                            "unknown reference table",
                        ));
                    }
                };
                load_reference_table_metadata(c, name, revision)?;
                if revision < after {
                    return Err(SparrowError::new(
                        ErrorCode::CodecViolation,
                        "reference table head moved behind the observed live revision",
                    )
                    .context("observed_revision", after.to_string())
                    .context("current_revision", revision.to_string()));
                }
                if revision == after {
                    Ok(None)
                } else {
                    load_reference_table_revision(c, name, revision).map(Some)
                }
            })();
            match result {
                Ok(value) => {
                    if let Err(error) = c.execute_batch("COMMIT") {
                        let _ = c.execute_batch("ROLLBACK");
                        return Err(db(error));
                    }
                    Ok(value)
                }
                Err(error) => {
                    let _ = c.execute_batch("ROLLBACK");
                    Err(error)
                }
            }
        })
    }

    /// Atomically apply a bounded batch to exactly the expected immutable
    /// revision.  It cannot create an absent table or change schema/keys.
    pub fn mutate_reference_table(
        &self,
        name: &str,
        mutation: &MutationSpec,
    ) -> Result<ReferenceTableRow> {
        check_name(name)?;
        mutation.validate()?;
        self.write(|c| {
            check_expected_head(c, name, mutation.expected_revision)?;
            let base = load_reference_table_revision(c, name, mutation.expected_revision)?;
            let table = mutation.apply(&base.table)?;
            let payload = table.encoded_bytes()?;
            self.publish_reference_table_locked(
                c,
                name,
                mutation.expected_revision,
                &table,
                &payload,
            )
        })
    }

    /// Copy retained historical contents into a newly allocated revision.
    /// Both the expected head and target are read under the write lock; a
    /// concurrent publication or GC cannot split validation from insertion.
    pub fn rollback_reference_table(
        &self,
        name: &str,
        rollback: &RollbackSpec,
    ) -> Result<ReferenceTableRow> {
        check_name(name)?;
        rollback.validate()?;
        self.write(|c| {
            check_expected_head(c, name, rollback.expected_revision)?;
            let current = load_reference_table_revision(c, name, rollback.expected_revision)?;
            let target = load_reference_table_revision(c, name, rollback.target_revision)?;
            // Hot followers bind a schema/key contract.  Rollback must not
            // silently alter it even when historical rows had another shape.
            if current.table.fields != target.table.fields
                || current.table.keys != target.table.keys
            {
                return Err(SparrowError::new(
                    ErrorCode::InvalidSchema,
                    "reference table rollback schema or keys differ from the current head",
                ));
            }
            let payload = target.table.encoded_bytes()?;
            self.publish_reference_table_locked(
                c,
                name,
                rollback.expected_revision,
                &target.table,
                &payload,
            )
        })
    }
}

fn check_expected_head(c: &Connection, name: &str, expected: u64) -> Result<()> {
    let current: Option<i64> = c
        .query_row(
            "SELECT latest_revision FROM reference_table_heads WHERE name=?1",
            [name],
            |r| r.get(0),
        )
        .optional()
        .map_err(db)?;
    let current = match current {
        None => 0,
        Some(revision) if revision > 0 => revision as u64,
        Some(_) => {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "reference table head revision is invalid",
            ));
        }
    };
    if current != expected {
        return Err(SparrowError::new(
            ErrorCode::InvalidArgument,
            "reference table expected revision does not match the current head",
        )
        .context("expected_revision", expected.to_string())
        .context("current_revision", current.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference_table::TableMutation;
    use serde_json::json;

    #[test]
    fn tab02_commit_busy_rolls_back_and_same_mutation_retry_recovers() {
        let directory = std::env::temp_dir().join(format!(
            "sparrow-tab02-commit-busy-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let path = directory.join("catalog.db");
        let writer = Store::open(&path).unwrap();
        let table = ReferenceTableSpec {
            fields: vec![
                sparrow_plan::graph::FieldSpec {
                    name: "id".into(),
                    data_type: "utf8".into(),
                    nullable: false,
                },
                sparrow_plan::graph::FieldSpec {
                    name: "value".into(),
                    data_type: "int64".into(),
                    nullable: false,
                },
            ],
            keys: vec!["id".into()],
            rows: vec![vec![json!("a"), json!(10)]],
        };
        let first = writer.publish_reference_table("sites", 0, &table).unwrap();
        {
            let connection = writer.inner.conn.lock().unwrap();
            // Change test-only connection behavior, never the production
            // timeout. A held SHARED reader permits BEGIN IMMEDIATE and the
            // bounded INSERT, but denies COMMIT's required EXCLUSIVE lock.
            connection
                .execute_batch("PRAGMA journal_mode=DELETE; PRAGMA busy_timeout=0;")
                .unwrap();
        }
        let reader = Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN DEFERRED").unwrap();
        let observed: i64 = reader
            .query_row(
                "SELECT latest_revision FROM reference_table_heads WHERE name='sites'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(observed, 1);

        let mutation = MutationSpec {
            expected_revision: 1,
            operations: vec![TableMutation::Upsert {
                row: vec![json!("a"), json!(20)],
            }],
        };
        // No races or sleep guesses: reader's SELECT has already acquired
        // its SHARED lock, and reader retains it until explicit ROLLBACK.
        let error = writer
            .mutate_reference_table("sites", &mutation)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Internal);
        assert!(error.message.contains("database is locked"), "{error}");
        assert!(error
            .context
            .iter()
            .any(|(key, value)| key == "catalog_stage" && value == "commit"));
        assert!(writer.inner.conn.lock().unwrap().is_autocommit());
        assert_eq!(writer.get_reference_table("sites").unwrap(), first);
        assert!(writer.get_reference_table_revision("sites", 2).is_err());
        assert_eq!(
            writer
                .list_reference_table_revisions("sites")
                .unwrap()
                .len(),
            1
        );
        reader.execute_batch("ROLLBACK").unwrap();

        let retried = writer.mutate_reference_table("sites", &mutation).unwrap();
        assert_eq!(retried.revision, 2);
        assert_eq!(retried.table.rows, vec![vec![json!("a"), json!(20)]]);
        assert_eq!(
            writer.get_reference_table_revision("sites", 1).unwrap(),
            first
        );
        assert!(writer.inner.conn.lock().unwrap().is_autocommit());
    }
}

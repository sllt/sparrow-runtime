//! Atomic incremental-table publication and metadata-only follower polling.
use crate::{
    reference_table_sha256, MutationSpec, ReferenceTableSpec, RollbackSpec, Store, TableMutation,
    MAX_REFERENCE_TABLE_BYTES, MAX_REFERENCE_TABLE_MUTATIONS, MAX_REFERENCE_TABLE_ROWS,
    MAX_REFERENCE_TABLE_VERSIONS,
};
use serde_json::{json, Value};
use sparrow_model::ErrorCode;
use sparrow_plan::graph::FieldSpec;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

fn table(value: i64) -> ReferenceTableSpec {
    ReferenceTableSpec {
        fields: vec![
            FieldSpec {
                name: "id".into(),
                data_type: "utf8".into(),
                nullable: false,
            },
            FieldSpec {
                name: "value".into(),
                data_type: "int64".into(),
                nullable: true,
            },
        ],
        keys: vec!["id".into()],
        rows: vec![
            vec![json!("a"), json!(value)],
            vec![json!("b"), json!(value + 1)],
        ],
    }
}

fn upsert(id: &str, value: Value) -> TableMutation {
    TableMutation::Upsert {
        row: vec![json!(id), value],
    }
}

fn mutation(expected_revision: u64, operations: Vec<TableMutation>) -> MutationSpec {
    MutationSpec {
        expected_revision,
        operations,
    }
}

#[test]
fn tab02_mutations_publish_one_immutable_revision_and_reject_replay() {
    let store = Store::open_memory().unwrap();
    let first = store
        .publish_reference_table("sites", 0, &table(10))
        .unwrap();
    let batch = mutation(
        1,
        vec![
            upsert("a", json!(20)),
            TableMutation::Delete {
                key: vec![json!("b")],
            },
            upsert("c", Value::Null),
        ],
    );
    let second = store.mutate_reference_table("sites", &batch).unwrap();
    assert_eq!(second.revision, 2);
    assert_eq!(
        second.table.rows,
        vec![vec![json!("a"), json!(20)], vec![json!("c"), Value::Null]]
    );
    assert_eq!(second.table.fields, first.table.fields);
    assert_eq!(second.table.keys, first.table.keys);
    assert_ne!(second.sha256, first.sha256);
    assert_eq!(
        store.get_reference_table_revision("sites", 1).unwrap(),
        first
    );
    let error = store.mutate_reference_table("sites", &batch).unwrap_err();
    assert!(error
        .context
        .iter()
        .any(|(key, value)| key == "current_revision" && value == "2"));
    assert_eq!(store.get_reference_table("sites").unwrap(), second);
    assert!(store.get_reference_table_revision("sites", 3).is_err());

    // No-op values are still an explicit publication with a new identity.
    let third = store
        .mutate_reference_table("sites", &mutation(2, vec![upsert("a", json!(20))]))
        .unwrap();
    assert_eq!(third.revision, 3);
    assert_eq!(third.table, second.table);
    assert_ne!(third.sha256, second.sha256);
}

#[test]
fn tab02_mutation_batch_validation_is_atomic_and_never_reflects_rows() {
    let store = Store::open_memory().unwrap();
    let first = store
        .publish_reference_table("sites", 0, &table(10))
        .unwrap();
    let marker = "private-business-row-marker";
    for operations in [
        vec![
            upsert("a", json!(20)),
            TableMutation::Delete {
                key: vec![json!(marker)],
            },
        ],
        vec![upsert("a", json!(20)), upsert("a", json!(30))],
        vec![
            upsert("a", json!(20)),
            TableMutation::Delete {
                key: vec![json!("a")],
            },
        ],
        vec![upsert("a", json!(20)), upsert(marker, json!(marker))],
        vec![TableMutation::Upsert {
            row: vec![json!(marker)],
        }],
        vec![TableMutation::Delete {
            key: vec![Value::Null],
        }],
        vec![TableMutation::Delete {
            key: vec![json!("a"), json!(marker)],
        }],
        vec![],
    ] {
        let error = store
            .mutate_reference_table("sites", &mutation(1, operations))
            .unwrap_err();
        assert!(!error.to_string().contains(marker));
        assert_eq!(store.get_reference_table("sites").unwrap(), first);
        assert_eq!(
            store.list_reference_table_revisions("sites").unwrap().len(),
            1
        );
    }
    assert!(store
        .mutate_reference_table("absent", &mutation(1, vec![upsert("a", json!(1))]))
        .is_err());
    for revision in [0, 2, u64::MAX] {
        assert!(store
            .mutate_reference_table("sites", &mutation(revision, vec![upsert("a", json!(20))]))
            .is_err());
    }
}

#[test]
fn tab02_mutation_budgets_cover_operations_payload_rows_and_retained_history() {
    let store = Store::open_memory().unwrap();
    store
        .publish_reference_table("sites", 0, &table(10))
        .unwrap();
    let operations = (0..=MAX_REFERENCE_TABLE_MUTATIONS)
        .map(|index| upsert(&format!("row-{index}"), json!(index)))
        .collect();
    assert_eq!(
        store
            .mutate_reference_table("sites", &mutation(1, operations))
            .unwrap_err()
            .code,
        ErrorCode::BoundExceeded
    );
    let huge = vec![upsert(&"x".repeat(MAX_REFERENCE_TABLE_BYTES), json!(1))];
    assert_eq!(
        store
            .mutate_reference_table("sites", &mutation(1, huge))
            .unwrap_err()
            .code,
        ErrorCode::MaxRecordSize
    );

    let mut full = table(1);
    full.rows = (0..MAX_REFERENCE_TABLE_ROWS)
        .map(|index| vec![json!(format!("r-{index}")), json!(index)])
        .collect();
    let snapshot = store.publish_reference_table("full", 0, &full).unwrap();
    assert!(store
        .mutate_reference_table("full", &mutation(1, vec![upsert("overflow", json!(1))]))
        .is_err());
    assert_eq!(store.get_reference_table("full").unwrap(), snapshot);

    // Result, not transient operation order, determines the final row cap.
    let replacement = store
        .mutate_reference_table(
            "full",
            &mutation(
                1,
                vec![
                    upsert("replacement", json!(1)),
                    TableMutation::Delete {
                        key: vec![json!("r-0")],
                    },
                ],
            ),
        )
        .unwrap();
    assert_eq!(replacement.row_count, MAX_REFERENCE_TABLE_ROWS as u64);

    for expected in 1..MAX_REFERENCE_TABLE_VERSIONS as u64 {
        store
            .mutate_reference_table(
                "sites",
                &mutation(expected, vec![upsert("a", json!(expected))]),
            )
            .unwrap();
    }
    assert!(store
        .mutate_reference_table(
            "sites",
            &mutation(
                MAX_REFERENCE_TABLE_VERSIONS as u64,
                vec![upsert("a", json!(0))]
            )
        )
        .is_err());
    assert_eq!(
        store.get_reference_table("sites").unwrap().revision,
        MAX_REFERENCE_TABLE_VERSIONS as u64
    );
}

#[test]
fn tab02_mutation_and_rollback_recover_from_injected_commit_failure() {
    let store = Store::open_memory().unwrap();
    let first = store
        .publish_reference_table("sites", 0, &table(10))
        .unwrap();
    store.debug_fail_next_commit();
    assert!(store
        .mutate_reference_table("sites", &mutation(1, vec![upsert("a", json!(20))]))
        .is_err());
    assert_eq!(store.get_reference_table("sites").unwrap(), first);
    assert!(store.get_reference_table_revision("sites", 2).is_err());
    let second = store
        .mutate_reference_table("sites", &mutation(1, vec![upsert("a", json!(20))]))
        .unwrap();
    store.debug_fail_next_commit();
    assert!(store
        .rollback_reference_table(
            "sites",
            &RollbackSpec {
                expected_revision: 2,
                target_revision: 1
            }
        )
        .is_err());
    assert_eq!(store.get_reference_table("sites").unwrap(), second);
    assert!(store.get_reference_table_revision("sites", 3).is_err());
    let third = store
        .rollback_reference_table(
            "sites",
            &RollbackSpec {
                expected_revision: 2,
                target_revision: 1,
            },
        )
        .unwrap();
    assert_eq!(third.revision, 3);
    assert_eq!(third.table, first.table);
    assert_ne!(third.sha256, first.sha256);
    assert!(store
        .rollback_reference_table(
            "sites",
            &RollbackSpec {
                expected_revision: 2,
                target_revision: 1
            }
        )
        .is_err());
    assert_eq!(
        store.get_reference_table_revision("sites", 2).unwrap(),
        second
    );
}

#[test]
fn tab02_mutation_final_payload_limit_is_checked_before_publication() {
    let store = Store::open_memory().unwrap();
    let mut strings = table(1);
    strings.fields[1].data_type = "utf8".into();
    strings.rows = vec![vec![json!("a"), json!("x".repeat(36 * 1024))]];
    let first = store
        .publish_reference_table("strings", 0, &strings)
        .unwrap();
    let batch = mutation(1, vec![upsert("new", json!("y".repeat(32 * 1024)))]);
    assert!(serde_json::to_vec(&batch).unwrap().len() < MAX_REFERENCE_TABLE_BYTES);
    assert_eq!(
        store
            .mutate_reference_table("strings", &batch)
            .unwrap_err()
            .code,
        ErrorCode::MaxRecordSize
    );
    assert_eq!(store.get_reference_table("strings").unwrap(), first);
    assert!(store.get_reference_table_revision("strings", 2).is_err());
}

#[test]
fn tab02_rollback_refuses_collected_future_and_incompatible_revisions() {
    let store = Store::open_memory().unwrap();
    store
        .publish_reference_table("sites", 0, &table(10))
        .unwrap();
    store
        .publish_reference_table("sites", 1, &table(20))
        .unwrap();
    assert!(store
        .rollback_reference_table(
            "sites",
            &RollbackSpec {
                expected_revision: 2,
                target_revision: 3
            }
        )
        .is_err());
    let mut changed = table(30);
    changed.fields[1].name = "renamed".into();
    store.publish_reference_table("sites", 2, &changed).unwrap();
    assert_eq!(
        store
            .rollback_reference_table(
                "sites",
                &RollbackSpec {
                    expected_revision: 3,
                    target_revision: 1
                }
            )
            .unwrap_err()
            .code,
        ErrorCode::InvalidSchema
    );
    store.gc_reference_table("sites").unwrap();
    assert!(store
        .rollback_reference_table(
            "sites",
            &RollbackSpec {
                expected_revision: 3,
                target_revision: 1
            }
        )
        .is_err());
    assert_eq!(store.get_reference_table("sites").unwrap().revision, 3);
}

#[test]
fn tab02_composite_keys_use_schema_types_and_key_order() {
    let store = Store::open_memory().unwrap();
    let spec = ReferenceTableSpec {
        fields: vec![
            FieldSpec {
                name: "flag".into(),
                data_type: "bool".into(),
                nullable: false,
            },
            FieldSpec {
                name: "number".into(),
                data_type: "int64".into(),
                nullable: false,
            },
            FieldSpec {
                name: "value".into(),
                data_type: "utf8".into(),
                nullable: false,
            },
        ],
        keys: vec!["number".into(), "flag".into()],
        rows: vec![vec![json!(true), json!(7), json!("old")]],
    };
    store
        .publish_reference_table("composite", 0, &spec)
        .unwrap();
    assert!(store
        .mutate_reference_table(
            "composite",
            &mutation(
                1,
                vec![TableMutation::Delete {
                    key: vec![json!(true), json!(7)]
                }]
            )
        )
        .is_err());
    let updated = store
        .mutate_reference_table(
            "composite",
            &mutation(
                1,
                vec![TableMutation::Upsert {
                    row: vec![json!(true), json!(7), json!("new")],
                }],
            ),
        )
        .unwrap();
    assert_eq!(updated.row_count, 1);
    let empty = store
        .mutate_reference_table(
            "composite",
            &mutation(
                2,
                vec![TableMutation::Delete {
                    key: vec![json!(7), json!(true)],
                }],
            ),
        )
        .unwrap();
    assert_eq!(empty.row_count, 0);
}

struct TestDb(PathBuf);
impl TestDb {
    fn new() -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "sparrow-tab02-{}-{}-{}",
            std::process::id(),
            crate::store::now_ms(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
    fn path(&self) -> PathBuf {
        self.0.join("catalog.db")
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn tab02_cross_connection_mutations_have_one_cas_winner() {
    let db = TestDb::new();
    let initial = Store::open(&db.path()).unwrap();
    initial
        .publish_reference_table("sites", 0, &table(10))
        .unwrap();
    let a = Store::open(&db.path()).unwrap();
    let b = Store::open(&db.path()).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let threads: Vec<_> = [(a, 20), (b, 30)]
        .into_iter()
        .map(|(store, value)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.mutate_reference_table("sites", &mutation(1, vec![upsert("a", json!(value))]))
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let loser = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .unwrap();
    assert!(loser
        .context
        .iter()
        .any(|(key, value)| key == "current_revision" && value == "2"));
    assert_eq!(initial.get_reference_table("sites").unwrap().revision, 2);
    assert_eq!(
        initial
            .list_reference_table_revisions("sites")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn tab02_follower_reads_share_snapshot_with_cross_connection_gc() {
    let db = TestDb::new();
    let writer = Store::open(&db.path()).unwrap();
    writer
        .publish_reference_table("sites", 0, &table(1))
        .unwrap();
    let reader = Store::open(&db.path()).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = barrier.clone();
    let task = std::thread::spawn(move || {
        writer_barrier.wait();
        for expected in 1..33 {
            writer
                .mutate_reference_table(
                    "sites",
                    &mutation(expected, vec![upsert("a", json!(expected + 1))]),
                )
                .unwrap();
            writer.gc_reference_table("sites").unwrap();
        }
    });
    barrier.wait();
    let mut after = 0;
    for _ in 0..100 {
        if let Some(update) = reader.reference_update_after("sites", after).unwrap() {
            assert!(update.revision > after);
            assert_eq!(update.table.rows[0][1], json!(update.revision));
            assert_eq!(
                update.sha256,
                reference_table_sha256("sites", update.revision, &update.table).unwrap()
            );
            after = update.revision;
        }
        std::thread::yield_now();
    }
    task.join().unwrap();
    let final_update = reader.reference_update_after("sites", after).unwrap();
    if let Some(update) = final_update {
        after = update.revision;
    }
    assert_eq!(after, 33);
    assert!(reader
        .reference_update_after("sites", 33)
        .unwrap()
        .is_none());
    assert!(reader.reference_update_after("missing", 0).is_err());
    assert!(reader.reference_update_after("sites", u64::MAX).is_err());
}

#[test]
fn tab02_follower_unchanged_path_is_metadata_only_and_changed_body_is_bounded() {
    let db = TestDb::new();
    let store = Store::open(&db.path()).unwrap();
    store
        .publish_reference_table("sites", 0, &table(1))
        .unwrap();
    let raw = rusqlite::Connection::open(db.path()).unwrap();
    raw.execute(
        "UPDATE reference_table_revisions SET table_json=?1 WHERE name='sites'",
        ["x".repeat(MAX_REFERENCE_TABLE_BYTES + 1)],
    )
    .unwrap();
    // The unchanged path must not parse/read row JSON.  Invalid metadata
    // still fails below; content integrity is rechecked whenever loaded.
    assert!(store.reference_update_after("sites", 1).unwrap().is_none());
    assert_eq!(
        store.reference_update_after("sites", 0).unwrap_err().code,
        ErrorCode::MaxRecordSize
    );
    raw.execute(
        "UPDATE reference_table_revisions SET sha256='invalid' WHERE name='sites'",
        [],
    )
    .unwrap();
    assert_eq!(
        store.reference_update_after("sites", 1).unwrap_err().code,
        ErrorCode::CodecViolation
    );
    // Every read failure rolled back its read transaction; unrelated writes
    // must continue to work rather than leaving BEGIN active.
    store
        .publish_reference_table("other", 0, &table(2))
        .unwrap();
}

#[test]
fn tab02_follower_rejects_regressed_head_instead_of_silently_retaining_stale_data() {
    let db = TestDb::new();
    let store = Store::open(&db.path()).unwrap();
    store
        .publish_reference_table("sites", 0, &table(1))
        .unwrap();
    store
        .mutate_reference_table("sites", &mutation(1, vec![upsert("a", json!(2))]))
        .unwrap();
    assert!(store.reference_update_after("sites", 2).unwrap().is_none());
    let raw = rusqlite::Connection::open(db.path()).unwrap();
    raw.execute(
        "UPDATE reference_table_heads SET latest_revision=1 WHERE name='sites'",
        [],
    )
    .unwrap();
    let error = store.reference_update_after("sites", 2).unwrap_err();
    assert_eq!(error.code, ErrorCode::CodecViolation);
    assert!(error
        .context
        .iter()
        .any(|(key, value)| key == "observed_revision" && value == "2"));
    assert!(error
        .context
        .iter()
        .any(|(key, value)| key == "current_revision" && value == "1"));
    // The failed read snapshot does not leave its transaction open.
    store
        .publish_reference_table("other", 0, &table(3))
        .unwrap();
}

#[test]
fn tab02_mutation_wire_shape_is_strict() {
    for body in [
        json!({"expected_revision":1}),
        json!({"expected_revision":1,"operations":[],"unknown":true}),
        json!({"expected_revision":1,"operations":[{"op":"upsert","row":[],"key":[]}]}),
        json!({"expected_revision":1,"operations":[{"op":"delete","row":[]}]}),
        json!({"expected_revision":1,"operations":[{"op":"truncate"}]}),
    ] {
        assert!(serde_json::from_value::<MutationSpec>(body).is_err());
    }
    assert!(serde_json::from_value::<RollbackSpec>(
        json!({"expected_revision":1,"target_revision":1,"unknown":true})
    )
    .is_err());
}

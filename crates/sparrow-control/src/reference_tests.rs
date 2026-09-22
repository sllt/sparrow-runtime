use serde_json::json;
use sparrow_plan::graph::FieldSpec;

use crate::{
    reference_table_sha256, ReferenceTableSpec, Store, MAX_REFERENCE_TABLE_BYTES,
    MAX_REFERENCE_TABLE_PREVIEW_PINS, MAX_REFERENCE_TABLE_ROWS,
};

fn table(site: &str) -> ReferenceTableSpec {
    ReferenceTableSpec {
        fields: vec![
            FieldSpec {
                name: "device_id".into(),
                data_type: "utf8".into(),
                nullable: false,
            },
            FieldSpec {
                name: "site".into(),
                data_type: "utf8".into(),
                nullable: true,
            },
        ],
        keys: vec!["device_id".into()],
        rows: vec![vec![json!("edge-a"), json!(site)]],
    }
}

fn pipeline(name: &str, revision: u64, sha256: &str) -> crate::PipelineSpec {
    let mut references = serde_json::Map::new();
    references.insert(
        name.to_string(),
        json!({"revision": revision, "sha256": sha256}),
    );
    let value = json!({
        "version": 1,
        "stream": "sensors",
        "sql": "SELECT device_id FROM sensors",
        "reference_tables": references,
        "source": {"kind": "mqtt", "host": "127.0.0.1", "port": 1883},
        "sink": {"kind": "log"}
    });
    crate::PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

#[test]
fn core_b_reference_table_publish_is_immutable_and_cas_protected() {
    let store = Store::open_memory().unwrap();
    let first = store
        .publish_reference_table("sites", 0, &table("west"))
        .unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(store.get_reference_table("sites").unwrap().revision, 1);
    assert_eq!(
        store
            .get_reference_table_revision("sites", 1)
            .unwrap()
            .table
            .rows,
        table("west").rows
    );

    let stale = store.publish_reference_table("sites", 0, &table("east"));
    assert!(stale.is_err(), "stale CAS must not overwrite the head");
    let second = store
        .publish_reference_table("sites", 1, &table("east"))
        .unwrap();
    assert_eq!(second.revision, 2);
    assert_eq!(
        store
            .get_reference_table_revision("sites", 1)
            .unwrap()
            .table
            .rows,
        table("west").rows
    );
    assert_eq!(store.get_reference_table("sites").unwrap().revision, 2);
}

#[test]
fn core_b_reference_table_publish_rejects_duplicate_keys_bad_rows_and_oversize() {
    let store = Store::open_memory().unwrap();

    let mut duplicate = table("west");
    duplicate.rows.push(vec![json!("edge-a"), json!("east")]);
    assert!(store
        .publish_reference_table("duplicate", 0, &duplicate)
        .is_err());

    let mut bad_width = table("west");
    bad_width.rows[0].pop();
    assert!(store
        .publish_reference_table("bad-width", 0, &bad_width)
        .is_err());

    let mut bad_type = table("west");
    bad_type.rows[0][1] = json!(42);
    assert!(store
        .publish_reference_table("bad-type", 0, &bad_type)
        .is_err());

    let mut too_many = table("west");
    too_many.rows = (0..=MAX_REFERENCE_TABLE_ROWS)
        .map(|i| vec![json!(format!("edge-{i}")), json!("west")])
        .collect();
    assert!(store
        .publish_reference_table("too-many", 0, &too_many)
        .is_err());

    let mut too_large = table("x");
    too_large.rows[0][1] = json!("x".repeat(MAX_REFERENCE_TABLE_BYTES));
    assert!(store
        .publish_reference_table("too-large", 0, &too_large)
        .is_err());
}

#[test]
fn core_b_pipeline_reference_revision_is_pinned_and_gc_keeps_latest_and_history() {
    let store = Store::open_memory().unwrap();
    let v1 = store
        .publish_reference_table("sites", 0, &table("west"))
        .unwrap();
    let spec = pipeline("sites", v1.revision, &v1.sha256);
    let row = store.put_pipeline("lookup", &spec, None).unwrap();
    assert_eq!(row.latest_revision, 1);
    assert_eq!(
        store.reference_bindings(&spec).unwrap()[0].sha256,
        v1.sha256
    );

    let v2 = store
        .publish_reference_table("sites", 1, &table("east"))
        .unwrap();
    let _v3 = store
        .publish_reference_table("sites", v2.revision, &table("north"))
        .unwrap();
    let gc = store.gc_reference_table("sites").unwrap();
    assert_eq!(gc["deleted"], 1);
    assert_eq!(gc["pinned"], true);
    assert!(store.get_reference_table_revision("sites", 1).is_ok());
    assert!(store.get_reference_table_revision("sites", 2).is_err());
    assert_eq!(store.get_reference_table("sites").unwrap().revision, 3);
}

#[test]
fn core_b_reference_table_publish_rolls_back_before_head_advance() {
    let store = Store::open_memory().unwrap();
    store
        .publish_reference_table("sites", 0, &table("west"))
        .unwrap();
    store.debug_fail_next_commit();
    assert!(store
        .publish_reference_table("sites", 1, &table("east"))
        .is_err());
    assert_eq!(store.get_reference_table("sites").unwrap().revision, 1);
    assert!(store.get_reference_table_revision("sites", 2).is_err());
}

#[test]
fn core_b_reference_table_binding_requires_the_stored_digest() {
    let store = Store::open_memory().unwrap();
    let row = store
        .publish_reference_table("sites", 0, &table("west"))
        .unwrap();
    let mut spec = pipeline("sites", row.revision, &row.sha256);
    spec.reference_tables
        .get_mut("sites")
        .expect("binding")
        .sha256 = "0".repeat(64);
    assert!(store.reference_bindings(&spec).is_err());
}

#[test]
fn core_b_reference_catalog_migrates_v2_to_v3_without_data_loss() {
    let path = std::env::temp_dir().join(format!(
        "sparrow-reference-v2-{}-{}.db",
        std::process::id(),
        crate::store::now_ms()
    ));
    let _ = std::fs::remove_file(&path);
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta VALUES ('catalog_schema_version','2'), ('format_version','1');",
            )
            .unwrap();
    }
    let store = Store::open(&path).unwrap();
    assert_eq!(store.schema_version().unwrap(), crate::CATALOG_SCHEMA_VERSION);
    store
        .publish_reference_table("sites", 0, &table("west"))
        .unwrap();
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[test]
fn core_b_future_reference_catalog_is_rejected_before_schema_mutation() {
    let path = std::env::temp_dir().join(format!(
        "sparrow-reference-future-{}-{}.db",
        std::process::id(),
        crate::store::now_ms()
    ));
    let _ = std::fs::remove_file(&path);
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta VALUES ('catalog_schema_version','99');",
            )
            .unwrap();
    }
    assert!(Store::open(&path).is_err());
    let connection = rusqlite::Connection::open(&path).unwrap();
    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='reference_table_revisions'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(table_count, 0);
    let _ = std::fs::remove_file(path);
}

#[test]
fn core_b_reference_dependency_preview_is_metadata_only_and_gc_is_not_truncated() {
    let store = Store::open_memory().unwrap();
    let v1 = store
        .publish_reference_table("sites", 0, &table("west"))
        .unwrap();
    let spec = pipeline("sites", v1.revision, &v1.sha256);
    for index in 0..=MAX_REFERENCE_TABLE_PREVIEW_PINS {
        store
            .put_pipeline(&format!("lookup-{index}"), &spec, None)
            .unwrap();
    }
    let preview = store.reference_table_dependencies("sites").unwrap();
    assert!(preview["revisions"][0]
        .as_object()
        .unwrap()
        .get("table")
        .is_none());
    assert_eq!(
        preview["pins"]["total_count"],
        (MAX_REFERENCE_TABLE_PREVIEW_PINS + 1) as u64
    );
    assert_eq!(
        preview["pins"]["returned_count"],
        MAX_REFERENCE_TABLE_PREVIEW_PINS as u64
    );
    assert_eq!(preview["pins"]["truncated"], true);

    let v2 = store
        .publish_reference_table("sites", 1, &table("east"))
        .unwrap();
    store
        .publish_reference_table("sites", v2.revision, &table("north"))
        .unwrap();
    let gc = store.gc_reference_table("sites").unwrap();
    assert_eq!(gc["deleted"], 1);
    assert!(store.get_reference_table_revision("sites", 1).is_ok());
}

#[test]
fn core_b_v3_catalog_missing_pin_table_fails_closed() {
    let path = std::env::temp_dir().join(format!(
        "sparrow-reference-missing-pin-{}-{}.db",
        std::process::id(),
        crate::store::now_ms()
    ));
    let _ = std::fs::remove_file(&path);
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        let table = table("west");
        let payload = table.encoded_bytes().unwrap();
        let sha256 = reference_table_sha256("sites", 1, &table).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta VALUES ('catalog_schema_version','3'), ('format_version','1');",
            )
            .unwrap();
        // Simulate a damaged v3 catalog where a historical table/head exists
        // but the dependency relation disappeared.  Recreating an empty pin
        // table would make GC unsafe: it could delete a revision still needed
        // by a persisted pipeline.  Store::open must fail closed instead.
        connection
            .execute_batch(
                "CREATE TABLE reference_table_revisions(
                    name TEXT NOT NULL, revision INTEGER NOT NULL,
                    table_json TEXT NOT NULL, sha256 TEXT NOT NULL,
                    payload_bytes INTEGER NOT NULL, row_count INTEGER NOT NULL,
                    created_at INTEGER NOT NULL, PRIMARY KEY(name, revision));
                 CREATE TABLE reference_table_heads(
                    name TEXT PRIMARY KEY, latest_revision INTEGER NOT NULL);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO reference_table_revisions
                    (name, revision, table_json, sha256, payload_bytes, row_count, created_at)
                 VALUES (?1, 1, ?2, ?3, ?4, ?5, 0)",
                rusqlite::params![
                    "sites",
                    String::from_utf8(payload).unwrap(),
                    sha256,
                    table.encoded_bytes().unwrap().len() as i64,
                    table.rows.len() as i64,
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO reference_table_heads(name, latest_revision) VALUES ('sites', 1)",
                [],
            )
            .unwrap();
    }
    assert!(Store::open(&path).is_err());
    let connection = rusqlite::Connection::open(&path).unwrap();
    let pin_table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='pipeline_reference_tables'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pin_table_count, 0);
    let revision_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM reference_table_revisions WHERE name='sites' AND revision=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(revision_count, 1);
    let _ = std::fs::remove_file(path);
}

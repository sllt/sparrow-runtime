use crate::{spec::PipelineSpec, store::Store, validate::bind_plan_with_store};
use crate::reference_table::{ReferenceTableRow, ReferenceTableSpec};
use serde_json::{json, Value};

fn store() -> Store {
    let store = Store::open_memory().unwrap();
    store.put_stream("sensors", &json!({"fields":[
        {"name":"device_id","type":"utf8","nullable":false},
        {"name":"v","type":"int64","nullable":false}
    ]}).to_string()).unwrap();
    store
}

fn publish(store: &Store, expected: u64, threshold: i64) -> ReferenceTableRow {
    let table: ReferenceTableSpec = serde_json::from_value(json!({
        "fields":[
            {"name":"device_id","type":"utf8","nullable":false},
            {"name":"threshold","type":"int64","nullable":false}
        ], "keys":["device_id"], "rows":[["a",threshold]]
    })).unwrap();
    store.publish_reference_table("limits", expected, &table).unwrap()
}

fn spec_json(table: &ReferenceTableRow) -> Value {
    json!({"version":1,"stream":"sensors",
        "reference_tables":{"limits":{"revision":table.revision,"sha256":table.sha256}},
        "source":{"kind":"file","path":"/tmp/sparrow/core-b-bind-not-opened.ndjson"},
        "sink":{"kind":"http","url":"http://127.0.0.1:9/unused"},
        "delivery":"live_best_effort","recovery":"restart_fresh",
        "graph":{"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
            {"id":2,"kind":"lookup","table":"limits",
                "on":[{"stream":"device_id","table":"device_id"}],"keep":["threshold"],"out":[3]},
            {"id":3,"kind":"capture_sink","name":"output"}
        ]}
    })
}

fn parse(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

#[test]
fn core_b_bound_lookup_uses_exact_revision_not_latest() {
    let store = store();
    let old = publish(&store, 0, 10);
    let spec = parse(spec_json(&old));
    let old_plan = bind_plan_with_store(&store, &spec, "lookup", 1).unwrap();
    let new = publish(&store, old.revision, 99);
    assert_ne!(old.sha256, new.sha256);
    let same_plan = bind_plan_with_store(&store, &spec, "lookup", 1).unwrap();
    assert_eq!(old_plan, same_plan);
    let bound = store.reference_bindings(&spec).unwrap();
    assert_eq!(bound.len(), 1);
    assert_eq!(bound[0].table.rows[0], vec![json!("a"), json!(10)]);
    assert_eq!(bound[0].sha256, old.sha256);
}

#[test]
fn core_b_binding_rejects_missing_digest_and_unused_dependency() {
    let store = store();
    let table = publish(&store, 0, 10);
    for mutation in ["absent", "digest", "revision", "unused"] {
        let mut value = spec_json(&table);
        match mutation {
            "absent" => value["reference_tables"] = json!({}),
            "digest" => value["reference_tables"]["limits"]["sha256"] = json!("0".repeat(64)),
            "revision" => value["reference_tables"]["limits"]["revision"] = json!(99),
            "unused" => {
                value["graph"]["nodes"] = json!([
                    {"id":1,"kind":"memory_source","table":"sensors","out":[3]},
                    {"id":3,"kind":"capture_sink","name":"output"}
                ]);
            }
            _ => unreachable!(),
        }
        assert!(bind_plan_with_store(&store, &parse(value), "lookup", 1).is_err(), "{mutation}");
    }
}

#[test]
fn core_b_binding_rejects_key_coercion_and_temporal_lookup() {
    let store = store();
    let table = publish(&store, 0, 10);
    for mutation in ["type", "key", "temporal"] {
        let mut value = spec_json(&table);
        match mutation {
            "type" => value["graph"]["nodes"][1]["on"][0]["stream"] = json!("v"),
            "key" => value["graph"]["nodes"][1]["on"][0]["table"] = json!("threshold"),
            "temporal" => {
                value["graph"]["nodes"][1]["temporal"] = json!(true);
                value["graph"]["nodes"][1]["as_of_field"] = json!("v");
            }
            _ => unreachable!(),
        }
        assert!(bind_plan_with_store(&store, &parse(value), "lookup", 1).is_err(), "{mutation}");
    }
}

#[test]
fn core_b_binding_rejects_inline_and_stream_schema_shadowing() {
    let store = store();
    let table = publish(&store, 0, 10);
    let mut value = spec_json(&table);
    value["graph"]["catalog"] = json!([{"name":"limits","fields":table.table.fields}]);
    assert!(bind_plan_with_store(&store, &parse(value), "lookup", 1).is_err());
    store.put_stream("limits", &json!({"fields":table.table.fields}).to_string()).unwrap();
    assert!(bind_plan_with_store(&store, &parse(spec_json(&table)), "lookup", 1).is_err());
}

#[test]
fn core_b_binding_aligned_and_bad_identity_fail_before_io() {
    let store = store();
    let table = publish(&store, 0, 10);
    for mutation in ["aligned", "zero", "uppercase", "short", "source_shadow"] {
        let mut value = spec_json(&table);
        match mutation {
            "aligned" => value["recovery"] = json!("aligned"),
            "zero" => value["reference_tables"]["limits"]["revision"] = json!(0),
            "uppercase" => value["reference_tables"]["limits"]["sha256"] = json!("A".repeat(64)),
            "short" => value["reference_tables"]["limits"]["sha256"] = json!("abc"),
            "source_shadow" => value["stream"] = json!("limits"),
            _ => unreachable!(),
        }
        assert!(PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).is_err(), "{mutation}");
    }
}

#[test]
fn core_b_shipped_reference_lookup_template_binds_published_identity() {
    let store = store();
    let publish: Value = serde_json::from_slice(include_bytes!(
        "../../../deploy/reference-table-limits.json")).unwrap();
    let table: ReferenceTableSpec = serde_json::from_value(publish["table"].clone()).unwrap();
    let revision = store.publish_reference_table("limits", 0, &table).unwrap();
    let mut value: Value = serde_json::from_slice(include_bytes!(
        "../../../deploy/pipeline-reference-lookup.json")).unwrap();
    // Shipped zero digest is intentionally not a usable identity. Publication
    // must provide the exact returned revision+digest before validation/start.
    assert!(bind_plan_with_store(&store, &parse(value.clone()), "template", 1).is_err());
    value["reference_tables"]["limits"] = json!({"revision":revision.revision,"sha256":revision.sha256});
    let plan = bind_plan_with_store(&store, &parse(value), "template", 1).unwrap();
    assert_eq!(plan.stages.iter().filter(|stage|
        matches!(stage, sparrow_plan::PhysicalStage::Lookup{..})).count(), 1);
}

#[test]
fn core_b_catalog_gc_and_publish_race_never_commits_dangling_binding() {
    let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("sparrow-core-b-race-{}-{stamp}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("catalog.db");
    let store = Store::open(&path).unwrap();
    for round in 0..16 {
        let expected = store.get_reference_table("limits").map_or(0, |row| row.revision);
        let previous = publish(&store, expected, 10);
        let _latest = publish(&store, previous.revision, 20);
        let spec = parse(spec_json(&previous));
        let name = format!("race_{round}");
        // Separate SQLite connections, not merely one in-process mutex.
        let publisher = Store::open(&path).unwrap();
        let collector = Store::open(&path).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let (published, collected) = std::thread::scope(|scope| {
            let gate = barrier.clone();
            let pipeline_name = name.clone();
            let publish = scope.spawn(move || {
                gate.wait(); publisher.put_pipeline(&pipeline_name, &spec, None)
            });
            let collect = scope.spawn(move || {
                barrier.wait(); collector.gc_reference_table("limits")
            });
            (publish.join().unwrap(), collect.join().unwrap())
        });
        assert!(published.is_ok() || collected.is_ok());
        if published.is_ok() {
            let persisted = store.get_pipeline(&name).unwrap();
            let resolved = store.reference_bindings(&persisted.spec).unwrap();
            assert_eq!(resolved.len(), 1);
            assert_eq!(resolved[0].sha256, previous.sha256);
        } else {
            assert!(store.get_pipeline(&name).is_err(), "failed dependency validation must roll back the pipeline revision");
        }
    }
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

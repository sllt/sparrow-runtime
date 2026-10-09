use sparrow_model::{DataType, Field, Schema};
use sparrow_plan::{AnalysisPlan, Catalog, PhysicalPlan, PhysicalStage};
fn catalog() -> Catalog {
    let mut c = Catalog::new();
    for name in ["l", "r"] {
        c.insert(
            name,
            Schema::new(
                1,
                vec![
                    Field::new(1, "k", DataType::Utf8, true),
                    Field::new(2, "v", DataType::Int64, false),
                    Field::new(3, "ts", DataType::Int64, false),
                    Field::new(4, "items", DataType::Dynamic, true),
                ],
            )
            .unwrap(),
        );
    }
    c
}
fn bind(sql: &str) -> sparrow_model::Result<PhysicalPlan> {
    crate::bind_sql(sql, &catalog(), 1.into(), 1.into())
        .map(|p| sparrow_plan::physicalize(&p, &Default::default()))
}
#[test]
fn analysis_sql_unnest_aliases_projection_and_ordinal_bind() {
    for sql in ["SELECT s.k, u.item, u.unnest_ordinal FROM l s CROSS JOIN UNNEST(s.items) AS u(item)",
        "SELECT k, x, ord FROM l s CROSS JOIN UNNEST(s.items) WITH ORDINALITY AS u(x, ord) WHERE CAST(x AS BIGINT) > 0"]{
        let plan=bind(sql).unwrap();assert!(plan.has_analysis());assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
        assert!(plan.stages.iter().any(|s|matches!(s,PhysicalStage::Analysis{plan,..} if matches!(plan.as_ref(),AnalysisPlan::Unnest{..}))));
    }
    for sql in [
        "SELECT * FROM l CROSS JOIN UNNEST(items, items) AS u(x)",
        "SELECT * FROM l CROSS JOIN UNNEST(v) AS u(x)",
        "SELECT * FROM l CROSS JOIN UNNEST(items) AS u(k)",
        "SELECT * FROM l CROSS JOIN UNNEST(items) AS u(x) LIMIT 1",
        "SELECT * FROM l CROSS JOIN UNNEST(items) WITH ORDINALITY AS u(x,x)",
        "SELECT * FROM l CROSS JOIN UNNEST(items) WITH ORDINALITY AS u(x,unnest_input)",
    ] {
        assert!(bind(sql).is_err(), "{sql}");
    }
}
#[test]
fn analysis_sql_join_range_window_left_and_strict_rejections() {
    for sql in ["SELECT a.v AS lv, b.v AS rv FROM l a JOIN r b ON a.k=b.k AND INTERVAL_MATCH(a.ts,b.ts,3,5)",
        "SELECT a.v,b.v FROM l a LEFT JOIN r b ON a.k=b.k AND WINDOW_MATCH(a.ts,b.ts,10,100)"]{
        let plan=bind(sql).unwrap();assert_eq!(plan.source_times.len(),2);assert!(plan.edges.is_some());assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
    }
    for condition in [
        "a.k=b.k AND INTERVAL_MATCH(a.ts,b.ts,3)",
        "a.k=b.k AND INTERVAL_MATCH(b.ts,a.ts,3,5)",
        "a.k=b.k AND WINDOW_MATCH(a.ts,b.ts,0)",
        "a.k=b.k OR WINDOW_MATCH(a.ts,b.ts,10)",
        "a.k=b.k AND a.k=b.k AND WINDOW_MATCH(a.ts,b.ts,10)",
        "a.k=b.k AND INTERVAL_MATCH(a.ts,b.ts,3,5) AND WINDOW_MATCH(a.ts,b.ts,10)",
    ] {
        assert!(
            bind(&format!("SELECT * FROM l a JOIN r b ON {condition}")).is_err(),
            "{condition}"
        );
    }
    assert!(bind("SELECT k FROM l a JOIN r b ON a.k=b.k AND WINDOW_MATCH(a.ts,b.ts,10)").is_err());
}
#[test]
fn analysis_sql_extended_aggregates_use_codec3_and_are_checked() {
    for f in [
        "first",
        "last",
        "var_pop",
        "var_samp",
        "stddev_pop",
        "stddev_samp",
    ] {
        let plan = bind(&format!(
            "SELECT {f}(v) AS value FROM l GROUP BY COUNT_WINDOW(8)"
        ))
        .unwrap();
        assert!(plan.has_extended_aggs());
        // Sub-batch 1: scalar inputs get participant codec 3 (v29/v30).
        let checkpoint = sparrow_plan::CheckpointPlan::from_physical(&plan).unwrap();
        assert!(checkpoint.has_extended_state());
        for args in ["*", "v,v", "DISTINCT v"] {
            assert!(bind(&format!(
                "SELECT {f}({args}) FROM l GROUP BY COUNT_WINDOW(8)"
            ))
            .is_err());
        }
    }
    assert!(bind("SELECT var_pop(k) FROM l GROUP BY COUNT_WINDOW(8)").is_err());
    // FIRST/LAST over nested/Dynamic values stays fresh-only.
    for f in ["first", "last"] {
        if let Ok(plan) = bind(&format!("SELECT {f}(items) AS value FROM l GROUP BY COUNT_WINDOW(8)")) {
            assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
        }
    }
    for f in [
        "array_length(items)",
        "object_get(items,'a')",
        "hex_encode(k)",
        "sha256(k)",
    ] {
        assert!(bind(&format!("SELECT {f} AS result FROM l")).is_ok());
    }
}

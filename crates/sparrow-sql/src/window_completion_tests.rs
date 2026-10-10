use crate::bind_sql;
use sparrow_model::{DataType, ErrorCode, Field, Schema, WindowKind};
use sparrow_plan::{Catalog, PhysicalStage, WindowSpec};

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.insert(
        "s",
        Schema::new(
            1,
            vec![
                Field::new(1, "k", DataType::Utf8, false),
                Field::new(2, "v", DataType::Int64, false),
                Field::new(3, "ts", DataType::Int64, false),
            ],
        )
        .unwrap(),
    );
    catalog
}
fn bind(assigner: &str) -> sparrow_model::Result<sparrow_plan::PhysicalPlan> {
    let sql = format!("SELECT k, COUNT(*) AS n, SUM(v) AS total FROM s GROUP BY k, {assigner}");
    bind_sql(&sql, &catalog(), 1.into(), 1.into())
        .map(|p| sparrow_plan::physicalize(&p, &Default::default()))
}
fn window(plan: &sparrow_plan::PhysicalPlan) -> &WindowSpec {
    plan.stages
        .iter()
        .find_map(|stage| match stage {
            PhysicalStage::WindowAgg { spec, .. } => Some(spec),
            _ => None,
        })
        .unwrap()
}

#[test]
fn windows_sql_binds_six_new_families_and_rejects_restore_manifest() {
    for (sql, expected) in [
        (
            "HOP(PROCESSING_TIME, 5, 10)",
            WindowKind::hopping_pt(10, 5).unwrap(),
        ),
        (
            "COUNT_WINDOW(5, 2)",
            WindowKind::sliding_count(5, 2).unwrap(),
        ),
        (
            "SLIDING(PROCESSING_TIME, 10)",
            WindowKind::sliding(10, 0, false).unwrap(),
        ),
        (
            "SLIDING(ts, 10, 5)",
            WindowKind::sliding(10, 5, true).unwrap(),
        ),
        (
            "SESSION(PROCESSING_TIME, 10, 100)",
            WindowKind::session(10, 100, false).unwrap(),
        ),
        (
            "SESSION(ts, 10, 100)",
            WindowKind::session(10, 100, true).unwrap(),
        ),
    ] {
        let plan = bind(sql).unwrap();
        assert_eq!(window(&plan).kind, expected, "{sql}");
        let layout =
            sparrow_plan::compat::PlanLayout::from_window(2.into(), 1.into(), window(&plan))
                .with_input_schema(catalog().get("s").unwrap());
        assert_eq!(
            sparrow_plan::compat::decide_state_reuse(&layout, &layout).as_str(),
            "reject"
        );
        assert_eq!(
            window(&plan).event_time_field.as_deref(),
            expected.uses_event_time().then_some("ts")
        );
        // Sub-batch 2a/2b: sliding count (v31/v32) and ET sliding/session
        // (File v33) have participant codec 4; PT buffered kinds have none.
        match sparrow_plan::CheckpointPlan::from_physical(&plan) {
            Ok(manifest) if sparrow_plan::checkpoint::checkpointable_buffered(expected) => {
                assert_eq!(manifest.states[0].codec, sparrow_plan::checkpoint::BUFFERED_WINDOW_STATE_CODEC);
                assert_eq!(manifest.recovery_prefix_len, None);
            }
            // Sub-batch 2c: PT hopping is the v34/v35 codec 1 participant.
            Ok(manifest) if matches!(expected, WindowKind::HoppingProcessingTime { .. }) => {
                assert_eq!(manifest.states[0].codec, sparrow_plan::checkpoint::WINDOW_STATE_CODEC);
                assert_eq!(manifest.states[0].window_kind, 4);
                assert!(manifest.has_pt_window_state());
            }
            result => assert_eq!(result.unwrap_err().code, ErrorCode::UnsupportedRestore, "{sql}"),
        }
    }
    assert_eq!(
        window(&bind("SLIDING(ts, INTERVAL '2' SECOND, INTERVAL '1' MILLISECOND)").unwrap()).kind,
        WindowKind::sliding(2_000_000, 1000, true).unwrap()
    );
    assert_eq!(
        window(&bind("COUNT_WINDOW(5)").unwrap()).kind,
        WindowKind::count(5).unwrap()
    );
}

#[test]
fn windows_sql_rejects_ambiguous_invalid_or_silently_ignored_parameters() {
    for sql in [
        "COUNT_WINDOW(0, 1)",
        "COUNT_WINDOW(3, 0)",
        "COUNT_WINDOW(3, 4)",
        "COUNT_WINDOW(1025, 1)",
        "COUNT_WINDOW(5, 2, 1)",
        "COUNT_WINDOW(DISTINCT 5, 2)",
        "HOP(PROCESSING_TIME, 5, 10, 0)",
        "SLIDING(ts, 0)",
        "SLIDING(ts, 10, -1)",
        "SLIDING(ts, 10, 0, 1)",
        "SESSION(ts, 10)",
        "SESSION(ts, 10, 0)",
        "SESSION(ts, 10, 100, 0)",
        "SLIDING(ts, INTERVAL 'oops' SECOND)",
        "SLIDING(ts, INTERVAL '2' YEAR)",
        "SLIDING(ts, INTERVAL '9223372036854775807' SECOND)",
        "SLIDING(ts, 10), SESSION(ts, 10, 100)",
        "SLIDING(ts, 10) OVER ()",
        "SLIDING(k, 10)",
        "SESSION(missing, 10, 100)",
    ] {
        assert!(bind(sql).is_err(), "unexpected acceptance: {sql}");
    }
}

#[test]
fn windows_graph_sql_kinds_and_resource_rejections_agree() {
    use serde_json::json;
    for (sql, w, et) in [
        (
            "COUNT_WINDOW(5, 2)",
            json!({"kind":"sliding_count","size":5,"step":2}),
            false,
        ),
        (
            "HOP(PROCESSING_TIME, 5, 10)",
            json!({"kind":"hop_pt","size_micros":10,"slide_micros":5}),
            false,
        ),
        (
            "SLIDING(ts, 10, 5)",
            json!({"kind":"sliding_et","size_micros":10,"delay_micros":5}),
            true,
        ),
        (
            "SESSION(ts, 10, 100)",
            json!({"kind":"session_et","gap_micros":10,"max_duration_micros":100}),
            true,
        ),
    ] {
        let raw = json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"s","out":[2]},
            {"id":2,"kind":"window_agg","keys":["k"],"window":w,"event_time_field":et.then_some("ts"),
                "aggs":[{"fn":"count","alias":"n"},{"fn":"sum","expr":{"k":"col","name":"v"},"alias":"total"}],"out":[3]},
            {"id":3,"kind":"capture_sink"}]});
        let parse = |value: &serde_json::Value| {
            sparrow_plan::bind_graph(
                &sparrow_plan::GraphSpec::from_json(&value.to_string()).unwrap(),
                &catalog(),
            )
        };
        let plan = sparrow_plan::physicalize(&parse(&raw).unwrap(), &Default::default());
        assert_eq!(window(&plan), window(&bind(sql).unwrap()));
        for (field, value) in [
            ("max_buffered_rows", json!(0)),
            ("max_buffered_rows", json!(16385)),
            ("gap_micros", json!(-1)),
        ] {
            let mut wrong = raw.clone();
            wrong["nodes"][1]["window"][field] = value;
            assert!(parse(&wrong).is_err(), "{sql}, {field}");
        }
    }
}

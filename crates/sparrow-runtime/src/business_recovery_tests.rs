//! Representative business chains, not an arbitrary combination matrix.
use super::*;

fn bind(nodes: serde_json::Value, fields: serde_json::Value) -> PhysicalPlan {
    let graph = sparrow_plan::GraphSpec::from_json(
        &json!({
            "version":1,"pipeline_id":99,"revision_id":1,
            "catalog":[{"name":"s","fields":fields}, {"name":"limits","fields":[
                {"name":"k","type":"utf8","nullable":false},
                {"name":"threshold","type":"int64","nullable":false}]}], "nodes":nodes
        })
        .to_string(),
    )
    .unwrap();
    sparrow_plan::physicalize(
        &sparrow_plan::bind_graph(&graph, &sparrow_plan::Catalog::new()).unwrap(),
        &Default::default(),
    )
}
fn values(plan: &PhysicalPlan, rows: &Output, name: &str) -> Vec<Scalar> {
    let PhysicalStage::CaptureSink { schema, .. } = plan.stages.last().unwrap() else {
        panic!()
    };
    let index = schema.index_of_name(name).unwrap();
    rows.iter().map(|(_, r)| r.values[index].clone()).collect()
}

#[test]
fn business_recovery_join_event_window_restores_pending_matches_and_aggregate() {
    let plan = bind(
        json!([
        {"id":1,"kind":"memory_source","table":"s","event_time_field":"ts","out_of_orderness_micros":10,"out":[3]},
        {"id":2,"kind":"memory_source","table":"s","event_time_field":"ts","out_of_orderness_micros":10,"out":[3]},
        {"id":3,"kind":"interval_join","stream_join":super::super::spec(JoinMode::Left,false),"out":[4]},
        {"id":4,"kind":"window_agg","event_time_field":"join_time","lateness_micros":0,"window":{"kind":"tumble_et","size_micros":100},
            "keys":["l_k"],"aggs":[{"fn":"sum","expr":{"k":"col","name":"l_v"},"alias":"total"}],"out":[5]},
        {"id":5,"kind":"change_detect","iot":{"keys":["l_k"],"fields":["total"],"emit_first":true,"ttl_micros":0,"max_keys":16,"invalid":"error"},"out":[6]},
        {"id":6,"kind":"capture_sink"}]),
        json!([{"name":"k","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false},{"name":"ts","type":"int64","nullable":false}]),
    );
    let d = vec![
        input(1, row("a", 1, 12), Some(2)),
        input(2, row("a", 101, 15), Some(5)),
        input(1, row("b", 2, 22), Some(12)),
        input(2, row("a", 102, 16), Some(6)),
        end(1),
        end(2),
    ];
    let (_, whole) = graph_segment(plan.clone(), None, &d, false);
    let (saved, mut head) = graph_segment(plan.clone(), None, &d[..3], true);
    assert_eq!(
        (saved.analysis.len(), saved.windows.len(), saved.iot.len()),
        (1, 1, 1)
    );
    let (_, tail) = graph_segment(plan.clone(), Some(saved), &d[3..], false);
    head.extend(tail);
    assert_eq!(head, whole);
    assert_eq!(
        values(&plan, &whole, "total"),
        vec![Scalar::Int64(2), Scalar::Int64(2)]
    );
    assert_eq!(
        values(&plan, &whole, "l_k"),
        vec![Scalar::utf8("a"), Scalar::utf8("b")]
    );
}

#[test]
fn business_recovery_multi_unnest_extended_count_and_change_state() {
    let plan = bind(
        json!([
        {"id":1,"kind":"memory_source","table":"s","out":[3]},
        {"id":2,"kind":"memory_source","table":"s","out":[3]},
        {"id":3,"kind":"union_all","out":[4]},
        {"id":4,"kind":"unnest","unnest":{"expr":{"k":"col","name":"items"}},"out":[5]},
        {"id":5,"kind":"window_agg","window":{"kind":"count","size":3},"keys":["k"],
            "aggs":[{"fn":"first","expr":{"k":"col","name":"item"},"alias":"first"},
                {"fn":"last","expr":{"k":"col","name":"item"},"alias":"last"}],"out":[6]},
        {"id":6,"kind":"change_detect","iot":{"keys":["k"],"fields":["last"],"emit_first":true,"ttl_micros":0,"max_keys":16,"invalid":"error"},"out":[7]},
        {"id":7,"kind":"capture_sink"}]),
        json!([{"name":"items","type":"array<int64>","nullable":true},{"name":"k","type":"utf8","nullable":false}]),
    );
    let keyed = |key: &str, values: &[i64]| {
        let mut row = array(values);
        row.values.push(Scalar::utf8(key));
        row
    };
    let d = vec![
        input(1, keyed("a", &[7, 8, 9]), None),
        input(2, keyed("b", &[]), None),
        input(1, keyed("a", &[7, 8]), None),
        input(1, keyed("a", &[9]), None),
        input(2, keyed("b", &[10, 11, 12]), None),
        end(1),
        end(2),
    ];
    let (_, whole) = graph_segment(plan.clone(), None, &d, false);
    let (saved, mut head) = graph_segment(plan.clone(), None, &d[..3], true);
    assert!(saved.plan.has_extended_state());
    let (_, tail) = graph_segment(plan.clone(), Some(saved), &d[3..], false);
    head.extend(tail);
    assert_eq!(head, whole);
    assert_eq!(
        values(&plan, &whole, "first"),
        vec![Scalar::Int64(7), Scalar::Int64(10)]
    );
    assert_eq!(
        values(&plan, &whole, "last"),
        vec![Scalar::Int64(9), Scalar::Int64(12)]
    );
}

fn reference() -> (Schema, Vec<Row>) {
    (
        Schema::new(
            90,
            vec![
                Field::new(1, "k", DataType::Utf8, false),
                Field::new(2, "threshold", DataType::Int64, false),
            ],
        )
        .unwrap(),
        vec![Row {
            values: vec![Scalar::utf8("a"), Scalar::Int64(10)],
        }],
    )
}

#[test]
fn business_recovery_pinned_reference_alarm_restores_deadline_episode_and_identity() {
    let plan = bind(
        json!([
        {"id":1,"kind":"memory_source","table":"s","out":[3]},
        {"id":2,"kind":"memory_source","table":"s","out":[3]},
        {"id":3,"kind":"union_all","out":[4]},
        {"id":4,"kind":"lookup","table":"limits","on":[{"stream":"k","table":"k"}],"keep":["threshold"],"out":[5]},
        {"id":5,"kind":"alarm","iot":{"keys":["k"],"fields":["enter","clear"],"emit_first":false,"ttl_micros":0,"max_keys":16,"invalid":"error",
            "timing":{"kind":"alarm","clock":"paused","activate_micros":150,"resolve_micros":50,"cooldown_micros":1000,"notification_max_age_micros":2000}},"out":[6]},
        {"id":6,"kind":"capture_sink"}]),
        json!([{"name":"k","type":"utf8","nullable":false},{"name":"enter","type":"bool","nullable":true},{"name":"clear","type":"bool","nullable":true}]),
    );
    let event = |enter, clear| Row {
        values: vec![Scalar::utf8("a"), Scalar::Bool(enter), Scalar::Bool(clear)],
    };
    let tick = || Decision {
        row: None,
        eof: None,
        idle: None,
    };
    let d = vec![
        input(1, event(true, false), None),
        tick(),
        tick(),
        input(2, event(false, true), None),
        tick(),
    ];
    let (_, whole) = graph_segment_with_table(plan.clone(), None, &d, false, Some(reference()));
    // Cut while Pending: downtime must not advance the paused alarm clock.
    let (saved, mut head) =
        graph_segment_with_table(plan.clone(), None, &d[..2], true, Some(reference()));
    let mut changed = saved.plan.clone();
    changed.reference_tables[0].revision += 1;
    assert!(saved.check_compatible(&changed).is_err());
    changed = saved.plan.clone();
    changed.reference_tables[0].canonical_sha256[0] ^= 1;
    assert!(saved.check_compatible(&changed).is_err());
    let (_, tail) =
        graph_segment_with_table(plan.clone(), Some(saved), &d[2..], false, Some(reference()));
    head.extend(tail);
    assert_eq!(head, whole);
    assert_eq!(whole.len(), 2);
    assert_eq!(
        values(&plan, &whole, "threshold"),
        vec![Scalar::Int64(10); 2]
    );
    assert_eq!(
        values(&plan, &whole, "sparrow_alarm_episode"),
        vec![Scalar::UInt64(1); 2]
    );
}

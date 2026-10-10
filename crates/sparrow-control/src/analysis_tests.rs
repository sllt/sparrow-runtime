use crate::*;
use serde_json::{json, Value};
use std::sync::Arc;
static QUERY_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
fn store() -> Arc<Store> {
    let s = Arc::new(Store::open_memory().unwrap());
    for name in ["l", "r"] {
        s.put_stream(name,r#"{"fields":[{"name":"k","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false},{"name":"ts","type":"int64","nullable":false},{"name":"items","type":"dynamic","nullable":true}]}"#).unwrap();
    }
    s
}
fn input() -> Value {
    json!([{"stream":"l","rows":[{"k":"a","v":1,"ts":10,"items":[7,8]},{"k":"b","v":2,"ts":30,"items":[]}]}])
}
async fn run(store: &Arc<Store>, sql: &str, inputs: Value) -> Value {
    let out = query::execute(
        store.clone(),
        serde_json::to_vec(&json!({"sql":sql,"inputs":inputs})).unwrap(),
    )
    .await
    .unwrap();
    serde_json::from_slice(out.as_ref()).unwrap()
}
#[tokio::test]
async fn analysis_query_unnest_join_and_moments_produce_reference_rows() {
    let _serial = QUERY_TEST.lock().await;
    let store = store();
    let result=run(&store,"SELECT s.k,to_int64(u.item) AS item,u.unnest_ordinal FROM l s CROSS JOIN UNNEST(s.items) AS u(item)",input()).await;
    assert_eq!(
        result["rows"],
        json!([{"k":"a","item":7,"unnest_ordinal":1},{"k":"a","item":8,"unnest_ordinal":2}])
    );
    assert_eq!(result["side_effects"], false);
    let mut inputs = input();
    inputs
        .as_array_mut()
        .unwrap()
        .push(json!({"stream":"r","rows":[{"k":"a","v":3,"ts":15}]}));
    for marker in [
        "INTERVAL_MATCH(a.ts,b.ts,3,5)",
        "WINDOW_MATCH(a.ts,b.ts,10)",
    ] {
        let result = run(
            &store,
            &format!("SELECT a.v AS lv,b.v AS rv FROM l a LEFT JOIN r b ON a.k=b.k AND {marker}"),
            inputs.clone(),
        )
        .await;
        let mut rows = result["rows"].as_array().unwrap().clone();
        rows.sort_by_key(|v| v["lv"].as_i64());
        assert_eq!(
            rows,
            json!([{"lv":1,"rv":3},{"lv":2,"rv":null}])
                .as_array()
                .unwrap()
                .clone()
        );
    }
    let rows: Vec<_> = [2, 4, 4, 4, 5, 5, 7, 9]
        .iter()
        .map(|v| json!({"k":"a","v":v,"ts":0}))
        .collect();
    let result=run(&store,"SELECT FIRST(v) AS first,LAST(v) AS last,VAR_POP(v) AS variance,STDDEV_POP(v) AS std FROM l GROUP BY COUNT_WINDOW(8)",json!([{"stream":"l","rows":rows}])).await;
    assert_eq!(
        result["rows"],
        json!([{"first":2,"last":9,"variance":4.0,"std":2.0}])
    );
    let ordered=query::execute(store.clone(),br#"{"sql":"SELECT object_keys(items) AS keys FROM l","inputs":[{"stream":"l","rows":[{"k":"a","v":1,"ts":0,"items":{"b":1,"a":2}}]}]}"#.to_vec()).await.unwrap();
    let ordered: Value = serde_json::from_slice(ordered.as_ref()).unwrap();
    assert_eq!(ordered["rows"][0]["keys"], json!(["b", "a"]));
}
#[tokio::test]
async fn analysis_query_limits_release_slots_and_response_owns_admission() {
    let _serial = QUERY_TEST.lock().await;
    let store = store();
    let bytes = serde_json::to_vec(&json!({"sql":"SELECT v FROM l","inputs":input()})).unwrap();
    let a = query::execute(store.clone(), bytes.clone()).await.unwrap();
    let b = query::execute(store.clone(), bytes.clone()).await.unwrap();
    assert!(
        matches!(query::execute(store.clone(),bytes.clone()).await,Err(e) if e.code==sparrow_model::ErrorCode::ResourceExhausted)
    );
    drop(a);
    drop(query::execute(store.clone(), bytes.clone()).await.unwrap());
    drop(b);
    for limits in [
        json!({"input_rows":1}),
        json!({"output_rows":1}),
        json!({"output_bytes":1}),
        json!({"work_units":1}),
        json!({"timeout_ms":0}),
    ] {
        let request = json!({"sql":"SELECT v FROM l","inputs":input(),"limits":limits});
        assert!(
            query::execute(store.clone(), serde_json::to_vec(&request).unwrap())
                .await
                .is_err()
        );
    }
    assert!(query::execute(
        store.clone(),
        br#"{"sql":"SELECT v FROM l","sql":"SELECT k FROM l","inputs":[]}"#.to_vec()
    )
    .await
    .is_err());
    assert!(query::execute(store.clone(),serde_json::to_vec(&json!({"sql":"SELECT COUNT(*) FROM l GROUP BY TUMBLE(PROCESSING_TIME, 1000)","inputs":input()})).unwrap()).await.is_err());
    drop(query::execute(store, bytes).await.unwrap());
}
#[test]
fn analysis_control_recovery_gate_rejects_before_io() {
    let store = store();
    for sql in [
        "SELECT FIRST(v) FROM l GROUP BY COUNT_WINDOW(2)",
        "SELECT * FROM l CROSS JOIN UNNEST(items) AS u(item)",
    ] {
        let value = json!({"version":1,"stream":"l","sql":sql,"source":{"kind":"file","path":"/var/lib/sparrow/data/absent.ndjson","file_contract":"sealed"},"sink":{"kind":"http","url":"http://127.0.0.1:9/"},"recovery":"restart_fresh"});
        let mut spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        let plan = bind_plan_with_store(&store, &spec, "analysis", 1).unwrap();
        validate_aligned_plan(&spec, &plan).unwrap();
        assert_eq!(
            effective_guarantees_with_plan(&spec, &plan)["aligned_eligible"],
            false
        );
        spec.recovery = "aligned".into();
        assert!(validate_aligned_plan(&spec, &plan).is_err());
    }
}

#[cfg(feature = "demo-io")]
#[test]
fn analysis_control_two_real_files_join_to_required_http() {
    struct Dir(std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let dir = Dir(sparrow_connectors::ensure_default_data_root().join(format!(
        "analysis-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    std::fs::create_dir_all(&dir.0).unwrap();
    std::fs::write(
        dir.0.join("l.ndjson"),
        b"{\"k\":\"a\",\"v\":1,\"ts\":10}\n{\"k\":\"b\",\"v\":2,\"ts\":30}\n",
    )
    .unwrap();
    std::fs::write(dir.0.join("r.ndjson"), b"{\"k\":\"a\",\"v\":3,\"ts\":15}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async{
        let http=sparrow_connectors::HttpCapture::start().await.unwrap();let store=store();store.put_allow("127.0.0.1",http.port()).unwrap();
        let source=|name:&str|json!({"kind":"file","path":dir.0.join(format!("{name}.ndjson")),"file_contract":"sealed"});
        let sink=json!({"kind":"http","url":http.url()});
        let value=json!({"version":1,"stream":"l","recovery":"restart_fresh","fail_on_decode":true,"source":source("l"),"sink":sink,
            "sql":"SELECT a.v AS lv,b.v AS rv FROM l a LEFT JOIN r b ON a.k=b.k AND INTERVAL_MATCH(a.ts,b.ts,3,5,1000)",
            "graph_io":{"sources":{"1":source("l"),"2":source("r")},"sinks":{"6":sink}}});
        let spec=PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        store.put_pipeline("analysis",&spec,None).unwrap();let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();request_start(&store,"analysis","test").unwrap();
        let status=tokio::time::timeout(std::time::Duration::from_secs(8),async{
            loop{sup.converge_once().await.unwrap();let state=store.actual("analysis").unwrap();if matches!(state.status.as_str(),"completed"|"failed"){break state;}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();assert_eq!(status.status.as_str(),"completed","{status:?}");
        let mut rows:Vec<Value>=http.bodies().iter().flat_map(|b|serde_json::from_slice::<Vec<Value>>(b).unwrap()).collect();rows.sort_by_key(|r|r["lv"].as_i64());
        assert_eq!(rows,json!([{"lv":1,"rv":3},{"lv":2,"rv":null}]).as_array().unwrap().clone());
        sup.stop_all().await;http.stop().await;
    });
}

#[test]
fn analysis_recovery_control_profiles_and_declared_limits() {
    let store = store();
    let root = sparrow_connectors::ensure_default_data_root().join("analysis-profile-validation");
    let source = |name: &str| json!({"kind":"file","path":root.join(format!("{name}.ndjson")),"file_contract":"append_only"});
    let sink = json!({"kind":"http","url":"http://127.0.0.1:9/out"});
    let mut value = json!({"version":1,"stream":"l","recovery":"aligned","fail_on_decode":true,
        "sql":"SELECT to_int64(u.item) AS item,u.unnest_input,u.unnest_ordinal FROM l CROSS JOIN UNNEST(items) AS u(item)",
        "source":source("l"),"sink":sink,"checkpoint_dir":root.join("unnest"),"checkpoint":{"resume_latest":true}});
    let check = |value: &Value| {
        let spec = PipelineSpec::from_json(&serde_json::to_vec(value).unwrap()).unwrap();
        let plan = bind_plan_with_store(&store, &spec, "analysis", 1).unwrap();
        validate_aligned_plan(&spec, &plan).unwrap();
        assert_eq!(effective_guarantees_with_plan(&spec, &plan)["aligned_eligible"], true);
        (spec, plan)
    };
    let (_, plan) = check(&value);
    assert_eq!(sparrow_runtime::snapshot_version_for(&sparrow_plan::CheckpointPlan::from_physical(&plan).unwrap(), "file").unwrap(), 36);
    #[cfg(feature = "jetstream")]
    {
        let mut js = value.clone();
        js["source"] = json!({"kind":"jetstream","jetstream":{"servers":["nats://127.0.0.1:4222"],"namespace":"analysis","stream":"INPUT","consumer":"rows","ownership_bucket":"OWNERS"}});
        js["delivery"] = json!("checkpointed_at_least_once");
        let (_, plan) = check(&js);
        assert_eq!(sparrow_runtime::snapshot_version_for(&sparrow_plan::CheckpointPlan::from_physical(&plan).unwrap(), "jetstream-v1").unwrap(), 37);
    }
    for name in ["a", "b"] {
        store.put_stream(name, r#"{"fields":[{"name":"k","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false},{"name":"ts","type":"int64","nullable":false}]}"#).unwrap();
    }
    value["stream"] = json!("a");
    value["sql"] = json!("SELECT a.v AS lv,b.v AS rv FROM a LEFT JOIN b ON a.k=b.k AND INTERVAL_MATCH(a.ts,b.ts,3,5,10)");
    value["source"] = source("a");
    value["graph_io"] = json!({"sources":{"1":source("a"),"2":source("b")},"sinks":{"6":sink}});
    value["checkpoint"] = json!({"resume_latest":true,"interval_ms":100,"timeout_ms":5000});
    let (_, plan) = check(&value);
    assert_eq!(sparrow_runtime::snapshot_version_for(&sparrow_plan::CheckpointPlan::from_physical(&plan).unwrap(), sparrow_runtime::graph_cut::KIND).unwrap(), 38);
    value["checkpoint"]["resume_latest"] = json!(false);
    let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
    let plan = bind_plan_with_store(&store, &spec, "analysis", 1).unwrap();
    assert_eq!(validate_aligned_plan(&spec, &plan).unwrap_err().code, sparrow_model::ErrorCode::UnsupportedRestore);
}

/// Sub-batch 1 gate: FIRST/LAST/VAR/STDDEV admit aligned recovery only through
/// the strict linear v29 (File) profile here; every other combination is
/// rejected at validate time, before any checkpoint history or input I/O.
#[test]
fn extended_aggregate_recovery_gate_admits_only_the_strict_profile() {
    let store = store();
    let root = sparrow_connectors::ensure_default_data_root()
        .join(format!("ext-agg-gate-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let spec_for = |sql: &str, source: Value, sink: Value, dir: Option<&str>| {
        let mut value = json!({"version":1,"stream":"l","sql":sql,"source":source,"sink":sink,"recovery":"aligned"});
        if let Some(dir) = dir {
            value["checkpoint_dir"] = json!(root.join(dir).to_string_lossy());
        }
        PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
    };
    let file = json!({"kind":"file","path":root.join("in.ndjson").to_string_lossy(),"file_contract":"sealed"});
    let http = json!({"kind":"http","url":"http://127.0.0.1:9/"});
    let all = "FIRST(v) AS f,LAST(k) AS l,VAR_POP(v) AS vp,VAR_SAMP(v) AS vs,STDDEV_POP(v) AS sp,STDDEV_SAMP(v) AS ss,SUM(v) AS s";
    for window in ["COUNT_WINDOW(3)", "TUMBLE(ts, 1000)", "HOP(ts, 1000, 2000)"] {
        let sql = format!("SELECT {all} FROM l GROUP BY {window}");
        let spec = spec_for(&sql, file.clone(), http.clone(), Some("ok"));
        let plan = bind_plan_with_store(&store, &spec, "ext", 1).unwrap();
        validate_aligned_plan(&spec, &plan).unwrap_or_else(|e| panic!("{window}: {e:?}"));
        let guarantees = effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(guarantees["aligned_eligible"], true, "{window}");
        assert_eq!(guarantees["extended_aggregates"]["snapshot_version"], 29, "{window}");
        assert_eq!(guarantees["extended_aggregates"]["state_codec"], 3, "{window}");
    }
    let count = format!("SELECT {all} FROM l GROUP BY COUNT_WINDOW(3)");
    let rejected: Vec<(&str, PipelineSpec)> = vec![
        (
            "processing-time window",
            spec_for("SELECT FIRST(v) AS f FROM l GROUP BY TUMBLE(PROCESSING_TIME, 1000)", file.clone(), http.clone(), Some("pt")),
        ),
        (
            "Dynamic FIRST input",
            spec_for("SELECT FIRST(items) AS f FROM l GROUP BY COUNT_WINDOW(2)", file.clone(), http.clone(), Some("dyn")),
        ),
        ("missing checkpoint_dir", spec_for(&count, file.clone(), http.clone(), None)),
        ("non-HTTP sink", spec_for(&count, file.clone(), json!({"kind":"log"}), Some("log"))),
        (
            "live MQTT source",
            spec_for(&count, json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"topic":"t"}), http.clone(), Some("mqtt")),
        ),
    ];
    for (label, spec) in rejected {
        // Rejection at bind or validate is before any history/input I/O.
        let error = match bind_plan_with_store(&store, &spec, "ext", 1) {
            Ok(plan) => validate_aligned_plan(&spec, &plan).expect_err(label),
            Err(error) => error,
        };
        assert!(
            matches!(error.code, sparrow_model::ErrorCode::UnsupportedRestore | sparrow_model::ErrorCode::InvalidArgument),
            "{label}: {error:?}"
        );
    }
    // restart_fresh keeps working for every combination (not tightened).
    let mut fresh = spec_for("SELECT FIRST(items) AS f FROM l GROUP BY COUNT_WINDOW(2)", file.clone(), http.clone(), None);
    fresh.recovery = "restart_fresh".into();
    let plan = bind_plan_with_store(&store, &fresh, "ext", 1).unwrap();
    validate_aligned_plan(&fresh, &plan).unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

/// Sub-batch 2a gate: sliding count (COUNT_WINDOW(size, step)) admits aligned
/// recovery only through the strict single-window linear File v31 profile here
/// (JetStream v32 needs the jetstream feature); other new windows stay
/// restart_fresh and every rejected combination fails before history/input I/O.
#[test]
fn sliding_count_recovery_gate_admits_only_the_strict_profile() {
    let store = store();
    let root = sparrow_connectors::ensure_default_data_root()
        .join(format!("sliding-count-gate-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let spec_for = |sql: &str, source: Value, sink: Value, dir: Option<&str>| {
        let mut value = json!({"version":1,"stream":"l","sql":sql,"source":source,"sink":sink,"recovery":"aligned"});
        if let Some(dir) = dir {
            value["checkpoint_dir"] = json!(root.join(dir).to_string_lossy());
        }
        PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
    };
    let file = json!({"kind":"file","path":root.join("in.ndjson").to_string_lossy(),"file_contract":"sealed"});
    let http = json!({"kind":"http","url":"http://127.0.0.1:9/"});
    for aggs in ["COUNT(*) AS c,SUM(v) AS s,MIN(v) AS mn,MAX(v) AS mx", "FIRST(v) AS f,LAST(k) AS l,VAR_SAMP(v) AS vs,STDDEV_POP(v) AS sp"] {
        let sql = format!("SELECT k,{aggs} FROM l GROUP BY k, COUNT_WINDOW(5, 2)");
        let spec = spec_for(&sql, file.clone(), http.clone(), Some("ok"));
        let plan = bind_plan_with_store(&store, &spec, "sc", 1).unwrap();
        validate_aligned_plan(&spec, &plan).unwrap_or_else(|e| panic!("{aggs}: {e:?}"));
        let guarantees = effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(guarantees["aligned_eligible"], true, "{guarantees}");
        assert_eq!(guarantees["windows"]["sliding_count"]["snapshot_version"], 31);
        assert_eq!(guarantees["windows"]["sliding_count"]["state_codec"], 4);
    }
    let count = "SELECT k,SUM(v) AS s FROM l GROUP BY k, COUNT_WINDOW(5, 2)";
    let rejected: Vec<(&str, PipelineSpec)> = vec![
        ("PT sliding", spec_for("SELECT SUM(v) AS s FROM l GROUP BY SLIDING(PROCESSING_TIME, 10)", file.clone(), http.clone(), Some("pts"))),
        ("PT hopping", spec_for("SELECT SUM(v) AS s FROM l GROUP BY HOP(PROCESSING_TIME, 5, 10)", file.clone(), http.clone(), Some("pth"))),
        ("Dynamic input", spec_for("SELECT LAST(items) AS l FROM l GROUP BY COUNT_WINDOW(5, 2)", file.clone(), http.clone(), Some("dyn"))),
        ("missing checkpoint_dir", spec_for(count, file.clone(), http.clone(), None)),
        ("non-HTTP sink", spec_for(count, file.clone(), json!({"kind":"log"}), Some("log"))),
        ("live MQTT source", spec_for(count, json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"topic":"t"}), http.clone(), Some("mqtt"))),
    ];
    for (label, spec) in rejected {
        let error = match bind_plan_with_store(&store, &spec, "sc", 1) {
            Ok(plan) => validate_aligned_plan(&spec, &plan).expect_err(label),
            Err(error) => error,
        };
        assert!(
            matches!(error.code, sparrow_model::ErrorCode::UnsupportedRestore | sparrow_model::ErrorCode::InvalidArgument),
            "{label}: {error:?}"
        );
    }
    // JetStream v32 passes the control replay gate (regression: Phase B found
    // the JetStream gate still admitted only tumbling Count windows).
    #[cfg(feature = "jetstream")]
    {
        let js = json!({"kind":"jetstream","jetstream":{"servers":["nats://127.0.0.1:4222"],"namespace":"sc","stream":"INPUT","consumer":"c","ownership_bucket":"OWNERS"}});
        let value = json!({"version":1,"stream":"l","sql":count,"source":js,"sink":http,"recovery":"aligned",
            "delivery":"checkpointed_at_least_once","checkpoint_dir":root.join("js").to_string_lossy(),
            "checkpoint":{"interval_ms":60000,"timeout_ms":3000,"resume_latest":true}});
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap_or_else(|e| panic!("{e:?}"));
        let plan = bind_plan_with_store(&store, &spec, "sc", 1).unwrap();
        validate_aligned_plan(&spec, &plan).unwrap_or_else(|e| panic!("JetStream v32: {e:?}"));
        let guarantees = effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(guarantees["windows"]["sliding_count"]["snapshot_version"], 32, "{guarantees}");
    }
    let mut fresh = spec_for("SELECT LAST(items) AS l FROM l GROUP BY COUNT_WINDOW(5, 2)", file.clone(), http.clone(), None);
    fresh.recovery = "restart_fresh".into();
    let plan = bind_plan_with_store(&store, &fresh, "sc", 1).unwrap();
    validate_aligned_plan(&fresh, &plan).unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

/// Sub-batch 2b: ET sliding / ET session admit only the strict linear File
/// v33 profile; JetStream + ET, PT buffered kinds, multiple windows, DAG,
/// late side output and non-HTTP sinks stay refused before any history.
#[test]
fn et_buffered_recovery_gate_admits_only_the_strict_file_v33_profile() {
    let store = store();
    let root = sparrow_connectors::ensure_default_data_root()
        .join(format!("et-buffered-gate-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let spec_for = |sql: &str, source: Value, sink: Value, dir: Option<&str>| {
        let mut value = json!({"version":1,"stream":"l","sql":sql,"source":source,"sink":sink,"recovery":"aligned"});
        if let Some(dir) = dir {
            value["checkpoint_dir"] = json!(root.join(dir).to_string_lossy());
        }
        PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
    };
    let file = json!({"kind":"file","path":root.join("in.ndjson").to_string_lossy(),"file_contract":"sealed"});
    let http = json!({"kind":"http","url":"http://127.0.0.1:9/"});
    for window in ["SLIDING(ts, 4000, 700)", "SESSION(ts, 3500, 7000)"] {
        for aggs in ["COUNT(*) AS c,SUM(v) AS s,MIN(v) AS mn,MAX(v) AS mx", "FIRST(v) AS f,LAST(k) AS l,VAR_SAMP(v) AS vs"] {
            let sql = format!("SELECT k,{aggs} FROM l GROUP BY k, {window}");
            let spec = spec_for(&sql, file.clone(), http.clone(), Some("ok"));
            let plan = bind_plan_with_store(&store, &spec, "et", 1).unwrap();
            validate_aligned_plan(&spec, &plan).unwrap_or_else(|e| panic!("{window} {aggs}: {e:?}"));
            let guarantees = effective_guarantees_with_plan(&spec, &plan);
            assert_eq!(guarantees["aligned_eligible"], true, "{guarantees}");
            assert_eq!(guarantees["windows"]["event_time_buffered"]["snapshot_version"], 33, "{guarantees}");
            assert_eq!(guarantees["windows"]["event_time_buffered"]["state_codec"], 4);
            assert!(guarantees["windows"]["event_time_buffered"]["future_skew"].as_str().unwrap().contains("wall_clock"));
            assert!(guarantees["windows"]["event_time_buffered"]["timers"].as_str().unwrap().contains("no_idle_generator"));
        }
    }
    let session = "SELECT k,SUM(v) AS s FROM l GROUP BY k, SESSION(ts, 3500, 7000)";
    let rejected: Vec<(&str, PipelineSpec)> = vec![
        ("PT sliding", spec_for("SELECT SUM(v) AS s FROM l GROUP BY SLIDING(PROCESSING_TIME, 10)", file.clone(), http.clone(), Some("pts"))),
        ("PT session", spec_for("SELECT SUM(v) AS s FROM l GROUP BY SESSION(PROCESSING_TIME, 10, 100)", file.clone(), http.clone(), Some("ptss"))),
        ("Dynamic input", spec_for("SELECT LAST(items) AS l FROM l GROUP BY SESSION(ts, 10, 100)", file.clone(), http.clone(), Some("dyn"))),
        ("missing checkpoint_dir", spec_for(session, file.clone(), http.clone(), None)),
        ("non-HTTP sink", spec_for(session, file.clone(), json!({"kind":"log"}), Some("log"))),
        ("live MQTT source", spec_for(session, json!({"kind":"mqtt","host":"127.0.0.1","port":1883,"topic":"t"}), http.clone(), Some("mqtt"))),
    ];
    for (label, spec) in rejected {
        let error = match bind_plan_with_store(&store, &spec, "et", 1) {
            Ok(plan) => validate_aligned_plan(&spec, &plan).expect_err(label),
            Err(error) => error,
        };
        assert!(
            matches!(error.code, sparrow_model::ErrorCode::UnsupportedRestore | sparrow_model::ErrorCode::InvalidArgument),
            "{label}: {error:?}"
        );
    }
    #[cfg(feature = "jetstream")]
    {
        let js = json!({"kind":"jetstream","jetstream":{"servers":["nats://127.0.0.1:4222"],"namespace":"et","stream":"INPUT","consumer":"c","ownership_bucket":"OWNERS"}});
        let value = json!({"version":1,"stream":"l","sql":session,"source":js,"sink":http,"recovery":"aligned",
            "delivery":"checkpointed_at_least_once","checkpoint_dir":root.join("js").to_string_lossy(),
            "checkpoint":{"interval_ms":60000,"timeout_ms":3000,"resume_latest":true}});
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        let plan = bind_plan_with_store(&store, &spec, "et", 1).unwrap();
        let error = validate_aligned_plan(&spec, &plan).expect_err("JetStream + ET stays refused");
        assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore, "{error:?}");
    }
    let mut fresh = spec_for("SELECT LAST(items) AS l FROM l GROUP BY SESSION(ts, 10, 100)", file.clone(), http.clone(), None);
    fresh.recovery = "restart_fresh".into();
    let plan = bind_plan_with_store(&store, &fresh, "et", 1).unwrap();
    validate_aligned_plan(&fresh, &plan).unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

/// Sub-batch 2a end to end through the Supervisor: File source -> sliding count
/// (COUNT_WINDOW(3, 2)) -> required HTTP. A manual checkpoint publishes v31;
/// after a kill the restored job replays only the uncommitted suffix and its
/// outputs equal an independent per-key ring-buffer oracle.
#[cfg(feature = "demo-io")]
#[test]
fn sliding_count_file_v31_checkpoint_kill_restore_matches_oracle() {
    use std::io::Write;
    let root = sparrow_connectors::ensure_default_data_root()
        .join(format!("sliding-count-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let input = root.join("in.ndjson");
    let checkpoint = root.join("checkpoint");
    let row = |i: i64| format!("{{\"k\":\"{}\",\"v\":{},\"ts\":{i}}}\n", if i % 3 == 0 { "b" } else { "a" }, (i * 7) % 11 - 3);
    let oracle = |n: i64| -> Vec<Value> {
        let mut keys: std::collections::BTreeMap<String, (i64, std::collections::VecDeque<i64>)> = Default::default();
        let mut out = Vec::new();
        for i in 1..=n {
            let k = if i % 3 == 0 { "b" } else { "a" };
            let (count, ring) = keys.entry(k.into()).or_default();
            *count += 1;
            ring.push_back((i * 7) % 11 - 3);
            if ring.len() > 3 { ring.pop_front(); }
            if *count >= 3 && *count % 2 == 0 {
                out.push(json!({"k":k,"c":3,"s":ring.iter().sum::<i64>(),"f":ring[0],"l":ring[2]}));
            }
        }
        out
    };
    std::fs::write(&input, (1..=10).map(row).collect::<String>()).unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let value = json!({"version":1,"stream":"l","recovery":"aligned",
            "sql":"SELECT k,COUNT(*) AS c,SUM(v) AS s,FIRST(v) AS f,LAST(v) AS l FROM l GROUP BY k, COUNT_WINDOW(3, 2)",
            "source":{"kind":"file","path":input,"file_contract":"append_only","inbox_capacity":8},
            "sink":{"kind":"http","url":http.url(),"outbox_capacity":8,"batch_rows":8,"linger_ms":1},
            "checkpoint_dir":checkpoint,"checkpoint":{"resume_latest":true}});
        let spec = PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        store.put_pipeline("sc", &spec, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "sc", "test").unwrap();
        let outputs = |http: &sparrow_connectors::HttpCapture| -> Vec<Value> {
            http.bodies().iter().flat_map(|b| serde_json::from_slice::<Vec<Value>>(b).unwrap())
                .map(|r| json!({"k":r["k"],"c":r["c"],"s":r["s"],"f":r["f"],"l":r["l"]})).collect()
        };
        let wait = |n: usize| {
            let (sup, store, http) = (sup.clone(), store.clone(), &http);
            async move {
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while outputs(http).len() < n {
                        sup.converge_once().await.unwrap();
                        let actual = store.actual("sc").unwrap();
                        assert_ne!(actual.status, "failed", "{actual:?}");
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                }).await.expect("sliding count output deadline");
            }
        };
        let head = oracle(10);
        wait(head.len()).await;
        let checkpoint_id = sup.checkpoint_named("sc").await.unwrap();
        let inventory = sparrow_runtime::CheckpointStore::open_readonly(&checkpoint).unwrap().inventory().unwrap();
        let current = inventory.generations.iter().find(|g| g.current).expect("CURRENT");
        assert_eq!(current.metadata.as_ref().unwrap().version, sparrow_runtime::SLIDING_COUNT_FILE_SNAPSHOT_VERSION);
        let current_before = std::fs::read(checkpoint.join("CURRENT")).unwrap();
        let mut file = std::fs::OpenOptions::new().append(true).open(&input).unwrap();
        file.write_all((11..=20).map(row).collect::<String>().as_bytes()).unwrap();
        file.sync_all().unwrap();
        let all = oracle(20);
        wait(all.len()).await;
        sup.kill_named("sc").await.unwrap();
        request_start(&store, "sc", "test").unwrap();
        sup.converge_once().await.unwrap();
        // Replay of the uncommitted suffix only (File v31 has no output ids).
        let replay = all.len() - head.len();
        wait(all.len() + replay).await;
        let got = outputs(&http);
        assert_eq!(got[..all.len()], all[..], "first run vs oracle");
        assert_eq!(got[all.len()..], all[head.len()..], "restored run continues from the v31 cut");
        assert_eq!(
            sup.checkpoint_snapshot("sc").unwrap().unwrap().1.snapshot().restored_from,
            Some(checkpoint_id)
        );
        assert_eq!(std::fs::read(checkpoint.join("CURRENT")).unwrap(), current_before);
        sup.stop_all().await;
        http.stop().await;
    });
    let _ = std::fs::remove_dir_all(&root);
}

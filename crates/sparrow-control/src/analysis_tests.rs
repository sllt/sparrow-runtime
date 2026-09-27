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

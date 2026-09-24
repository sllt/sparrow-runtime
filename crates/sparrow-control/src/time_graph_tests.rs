//! Real multi-source/time/required-HTTP recovery; no evaluator-derived oracle.
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use sparrow_runtime::{graph_cut::GraphCut, CheckpointStore, PipelineSnapshot};
use std::{path::Path, sync::Arc, time::Duration};

struct Scratch(std::path::PathBuf);
impl Scratch {
    fn new() -> Self {
        let path = sparrow_connectors::ensure_default_data_root().join(format!(
            "time-graph-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        for name in ["a.ndjson", "b.ndjson"] {
            std::fs::write(path.join(name), b"").unwrap();
        }
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("s",r#"{"fields":[{"name":"v","type":"int64","nullable":false},{"name":"ts","type":"int64","nullable":false}]}"#).unwrap();
    store
}
fn configuration(dir: &Path, urls: [&str; 2], event: bool) -> Value {
    let source = |name: &str| json!({"kind":"file","path":dir.join(name),"file_contract":"append_only","inbox_capacity":1});
    let sink = |url: &str| json!({"kind":"http","url":url,"batch_rows":1,"linger_ms":0,"outbox_capacity":1});
    let mut nodes = json!([
        {"id":1,"kind":"memory_source","table":"s","out":[3]},
        {"id":2,"kind":"memory_source","table":"s","out":[3]},
        {"id":3,"kind":"union_all","out":[4]},
        {"id":4,"kind":"window_agg","window":{"kind":"processing_time","size_micros":1000000},"keys":[],
            "aggs":[{"fn":"sum","expr":{"k":"col","name":"v"},"alias":"total"}],"out":[5]},
        {"id":5,"kind":"branch","out":[6,7]},
        {"id":6,"kind":"capture_sink","name":"a"},{"id":7,"kind":"capture_sink","name":"b"}]);
    if event {
        for index in [0, 1] {
            nodes[index]["event_time_field"] = json!("ts");
            nodes[index]["out_of_orderness_micros"] = json!(0);
        }
        nodes[3]["window"] = json!({"kind":"event_time","size_micros":100,"event_time_field":"ts","lateness_micros":10});
    }
    json!({"stream":"s","source":source("a.ndjson"),"sink":sink(urls[0]),"fail_on_decode":true,
        "recovery":"aligned","checkpoint_dir":dir.join("checkpoints"),"checkpoint":{"interval_ms":100,"timeout_ms":4000,"resume_latest":true},
        "graph_io":{"sources":{"1":source("a.ndjson"),"2":source("b.ndjson")},"sinks":{"6":sink(urls[0]),"7":sink(urls[1])}},
        "graph":{"version":1,"pipeline_id":818,"revision_id":1,"nodes":nodes}})
}
fn parse(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}
fn append(dir: &Path, name: &str, v: i64, ts: i64) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.join(name))
        .unwrap();
    writeln!(file, "{{\"v\":{v},\"ts\":{ts}}}").unwrap();
    file.sync_all().unwrap();
}
fn snapshot(dir: &Path) -> Option<PipelineSnapshot> {
    CheckpointStore::open_readonly(dir.join("checkpoints"))
        .ok()?
        .recover_pipeline_required()
        .ok()
}
async fn wait_cut(
    sup: &Arc<Supervisor>,
    store: &Store,
    dir: &Path,
    condition: impl Fn(&GraphCut) -> bool,
) -> GraphCut {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            sup.converge_once().await.unwrap();
            let actual = store.actual("graph").unwrap();
            assert_ne!(actual.status, "failed", "{actual:?}");
            if let Some(saved) = snapshot(dir) {
                let cut = GraphCut::unwrap(&saved.source).unwrap();
                if condition(&cut) {
                    return cut;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("durable graph cut deadline")
}
fn outputs(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|b| serde_json::from_slice::<Vec<Value>>(b).unwrap())
        .collect()
}

#[test]
fn alarm_control_graph_branch_timers_union_replay_and_scoped_episodes() {
    use crate::paused_time_tests::HeldHttp;
    let dir = Scratch::new();
    std::fs::write(dir.0.join("a.ndjson"),b"{\"device_id\":\"a\",\"enter\":true,\"clear\":false}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http=HeldHttp::start().await;
        let store=Arc::new(Store::open_memory().unwrap());
        store.put_stream("s",r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"enter","type":"bool","nullable":true},{"name":"clear","type":"bool","nullable":true}]}"#).unwrap();
        store.put_allow("127.0.0.1",http.port()).unwrap();
        let mut config=configuration(&dir.0,[&http.url(),&http.url()],false);
        let iot=json!({"keys":["device_id"],"fields":["enter","clear"],"emit_first":false,"ttl_micros":0,"max_keys":8,"invalid":"ignore",
            "timing":{"kind":"alarm","clock":"paused","activate_micros":1000000,"resolve_micros":200000,"cooldown_micros":1000000,"notification_max_age_micros":2000000}});
        config["graph"]["nodes"]=json!([
            {"id":1,"kind":"memory_source","table":"s","out":[2]},
            {"id":2,"kind":"branch","out":[3,4]},
            {"id":3,"kind":"alarm","iot":iot,"out":[5]},
            {"id":4,"kind":"alarm","iot":iot,"out":[5]},
            {"id":5,"kind":"union_all","out":[6]},
            {"id":6,"kind":"capture_sink","name":"alarms"}]);
        config["graph_io"]["sources"].as_object_mut().unwrap().remove("2");
        config["graph_io"]["sinks"].as_object_mut().unwrap().remove("7");
        let spec=parse(config);store.put_pipeline("graph",&spec,None).unwrap();
        let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();request_start(&store,"graph","test").unwrap();
        wait_cut(&sup,&store,&dir.0,|c|c.ingested==1).await;http.wait(1).await;
        let before=http.outputs()[0].clone();let saved=snapshot(&dir.0).unwrap();
        assert_eq!(sparrow_runtime::snapshot_version_for(&saved.plan,&saved.source.identity.kind).unwrap(),22);
        assert_eq!(GraphCut::unwrap(&saved.source).unwrap().outputs[&6],1);
        sup.kill_named("graph").await.unwrap();request_start(&store,"graph","test").unwrap();sup.converge_once().await.unwrap();http.wait(2).await;
        assert_eq!(http.outputs()[1],before);http.release();
        wait_cut(&sup,&store,&dir.0,|c|c.outputs[&6]==3).await;
        let output=http.outputs();assert_eq!(output.len(),3);
        assert_ne!(output[0]["data"]["sparrow_alarm_operator"],output[2]["data"]["sparrow_alarm_operator"]);
        assert_eq!(output[0]["data"]["sparrow_alarm_episode"],1);assert_eq!(output[2]["data"]["sparrow_alarm_episode"],1);
        assert_eq!(output[0]["data"]["sparrow_alarm_generation"],output[2]["data"]["sparrow_alarm_generation"]);
        sup.kill_named("graph").await.unwrap();request_start(&store,"graph","test").unwrap();sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;assert_eq!(http.outputs(),output);
        sup.stop_all().await;http.stop().await;assert_eq!(kernel.admitted_jobs(),0);assert_eq!(kernel.live_tasks(),0);
    });
}

#[test]
fn time_graph_partial_required_sink_flush_replays_identical_ids_and_payloads() {
    use crate::paused_time_tests::HeldHttp;
    let dir = Scratch::new();
    append(&dir.0, "a.ndjson", 3, 10);
    append(&dir.0, "b.ndjson", 7, 20);
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let a = HeldHttp::start().await;
        let b = HeldHttp::start().await;
        a.release();
        let store = store();
        for port in [a.port(), b.port()] {
            store.put_allow("127.0.0.1", port).unwrap();
        }
        let spec = parse(configuration(&dir.0, [&a.url(), &b.url()], false));
        store.put_pipeline("graph", &spec, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "graph", "test").unwrap();
        wait_cut(&sup, &store, &dir.0, |c| c.ingested == 2).await;
        a.wait(1).await;
        b.wait(1).await;
        let current = snapshot(&dir.0).unwrap();
        assert!(GraphCut::unwrap(&current.source)
            .unwrap()
            .outputs
            .values()
            .all(|n| *n == 1));
        sup.kill_named("graph").await.unwrap();
        let before_a = a.outputs();
        let before_b = b.outputs();
        assert_eq!(before_a.len(), 1);
        assert_eq!(before_b.len(), 1);
        assert_eq!(before_a[0]["data"]["total"], 10);
        tokio::time::sleep(Duration::from_millis(150)).await;
        request_start(&store, "graph", "test").unwrap();
        sup.converge_once().await.unwrap();
        a.wait(2).await;
        b.wait(2).await;
        assert_eq!(a.outputs()[0], a.outputs()[1]);
        assert_eq!(b.outputs()[0], b.outputs()[1]);
        assert_ne!(a.outputs()[0]["id"], b.outputs()[0]["id"]);
        b.release();
        wait_cut(&sup, &store, &dir.0, |c| {
            c.outputs.values().all(|n| *n == 2)
        })
        .await;
        sup.stop_all().await;
        a.stop().await;
        b.stop().await;
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.live_tasks(), 0);
    });
}

#[test]
fn time_graph_missing_corrupt_log_and_policy_changes_refuse_restore() {
    for mutation in 0..4 {
        let dir = Scratch::new();
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = store();
            store.put_allow("127.0.0.1", http.port()).unwrap();
            let mut config = configuration(&dir.0, [&http.url(), &http.url()], false);
            store
                .put_pipeline("graph", &parse(config.clone()), None)
                .unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "graph", "test").unwrap();
            wait_cut(&sup, &store, &dir.0, |c| c.sequence >= 1).await;
            sup.kill_named("graph").await.unwrap();
            let pending = dir.0.join("checkpoints/TIME_PENDING");
            match mutation {
                0 => std::fs::remove_file(&pending).unwrap(),
                1 => {
                    let mut bytes = std::fs::read(&pending).unwrap();
                    bytes[10] ^= 1;
                    std::fs::write(&pending, bytes).unwrap();
                }
                2 => config["graph_io"]["idle_after_ms"] = json!(200),
                _ => {
                    config["graph_io"]["sources"]["1"]["file_contract"] = json!("sealed");
                    config["source"] = config["graph_io"]["sources"]["1"].clone();
                }
            }
            if mutation >= 2 {
                let etag = store.get_pipeline("graph").unwrap().etag;
                store
                    .put_pipeline("graph", &parse(config), Some(&etag))
                    .unwrap();
            }
            request_start(&store, "graph", "test").unwrap();
            sup.converge_once().await.unwrap();
            assert_ne!(
                store.actual("graph").unwrap().status,
                "running",
                "mutation {mutation}"
            );
            assert!(outputs(&http).is_empty());
            sup.stop_all().await;
            http.stop().await;
            assert_eq!(kernel.admitted_jobs(), 0);
        });
    }
}

#[test]
fn time_graph_et_explicit_idle_reactivation_preserves_watermark_floor() {
    let dir = Scratch::new();
    append(&dir.0, "a.ndjson", 2, 10);
    append(&dir.0, "a.ndjson", 3, 210);
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut config = configuration(&dir.0, [&http.url(), &http.url()], true);
        config["graph_io"]["idle_after_ms"] = json!(300);
        store.put_pipeline("graph", &parse(config), None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "graph", "test").unwrap();
        wait_cut(&sup, &store, &dir.0, |c| {
            c.sources.values().all(|s| s.progress.idle)
        })
        .await;
        append(&dir.0, "a.ndjson", 5, 310);
        let active = wait_cut(&sup, &store, &dir.0, |c| c.ingested == 3).await;
        assert!(active.sources[&2].progress.idle);
        let floor = active.unions[&3].emitted.watermark.unwrap();
        assert_eq!(floor, 310);
        sup.kill_named("graph").await.unwrap();
        append(&dir.0, "b.ndjson", 99, 10);
        request_start(&store, "graph", "test").unwrap();
        let cut = wait_cut(&sup, &store, &dir.0, |c| c.ingested == 4).await;
        assert!(!cut.sources[&2].progress.idle);
        assert_eq!(cut.unions[&3].emitted.watermark, Some(floor));
        assert!(
            outputs(&http).iter().all(|row| row["data"]["total"] != 99),
            "reactivation cannot reopen finalized event-time windows"
        );
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn time_graph_profile_matrix_and_negative_admission() {
    let dir = Scratch::new();
    let store = store();
    store.put_stream("dynamic_s",r#"{"fields":[{"name":"v","type":"int64","nullable":false},{"name":"ts","type":"int64","nullable":false},{"name":"unused","type":"dynamic","nullable":true}]}"#).unwrap();
    store
        .put_stream(
            "graph_signals",
            include_str!("../../../deploy/stream-time-graph.json"),
        )
        .unwrap();
    for template in [
        include_bytes!("../../../deploy/pipeline-time-graph-pt.json").as_slice(),
        include_bytes!("../../../deploy/pipeline-time-graph-et.json").as_slice(),
    ] {
        let mut spec = PipelineSpec::from_json(template).unwrap();
        let io = spec.graph_io.as_mut().unwrap();
        for (id, input) in &mut io.sources {
            input.path = Some(
                dir.0
                    .join(if *id == 1 { "a.ndjson" } else { "b.ndjson" })
                    .to_string_lossy()
                    .into(),
            );
        }
        spec.source = io.sources[&1].clone();
        spec.checkpoint_dir = Some(dir.0.join("checkpoints").to_string_lossy().into());
        let plan = crate::bind_plan_with_store(&store, &spec, "template", 1).unwrap();
        crate::validate_aligned_plan(&spec, &plan).unwrap();
    }
    for event in [false, true] {
        let value = configuration(
            &dir.0,
            ["http://127.0.0.1:1/a", "http://127.0.0.1:1/b"],
            event,
        );
        let spec = parse(value);
        let plan = crate::bind_plan_with_store(&store, &spec, "graph", 1).unwrap();
        crate::validate_aligned_plan(&spec, &plan).unwrap();
        let mut dynamic = configuration(
            &dir.0,
            ["http://127.0.0.1:1/a", "http://127.0.0.1:1/b"],
            event,
        );
        dynamic["stream"] = json!("dynamic_s");
        for i in [0, 1] {
            dynamic["graph"]["nodes"][i]["table"] = json!("dynamic_s");
        }
        let dynamic_spec = parse(dynamic.clone());
        let dynamic_plan =
            crate::bind_plan_with_store(&store, &dynamic_spec, "dynamic", 1).unwrap();
        assert!(crate::validate_aligned_plan(&dynamic_spec, &dynamic_plan)
            .unwrap_err()
            .message
            .contains("input fingerprint requires scalar"));
        let mut fresh = dynamic_spec.clone();
        fresh.recovery = "restart_fresh".into();
        crate::validate_aligned_plan(&fresh, &dynamic_plan).unwrap();
        if !event {
            let nodes = dynamic["graph"]["nodes"].as_array().unwrap();
            let mut source = nodes[0].clone();
            source["out"] = json!([4]);
            let mut window = nodes[3].clone();
            window["out"] = json!([6]);
            dynamic["graph"]["nodes"] = json!([source, window, nodes[5].clone()]);
            dynamic.as_object_mut().unwrap().remove("graph_io");
            let linear = parse(dynamic);
            let plan = crate::bind_plan_with_store(&store, &linear, "dynamic-linear", 1).unwrap();
            assert!(plan.edges.is_none());
            assert!(crate::validate_aligned_plan(&linear, &plan)
                .unwrap_err()
                .message
                .contains("input fingerprint requires scalar"));
        }
        let manifest = sparrow_plan::CheckpointPlan::from_physical(&plan).unwrap();
        assert!(manifest.is_time_graph());
        let effective = crate::effective_guarantees_with_plan(&spec, &plan);
        assert_eq!(effective["aligned_eligible"], true);
        assert_eq!(
            effective["checkpoint_participants"]["snapshot_version"],
            if event { 19 } else { 18 }
        );
        let mut live = spec.clone();
        live.recovery = "restart_fresh".into();
        crate::validate_aligned_plan(&live, &plan).unwrap();
        let effective = crate::effective_guarantees_with_plan(&live, &plan);
        assert_eq!(effective["aligned_eligible"], true);
        assert!(effective["checkpoint_participants"]["snapshot_version"].is_null());
        assert_eq!(effective["graph_time"]["durable_rounds"], false);
        assert_eq!(effective["graph_time"]["stable_output_ids"], false);
        assert_eq!(
            effective["recovery_risk"],
            "restart_fresh_loses_graph_state"
        );
        for mutation in 0..8 {
            let mut wrong = spec.clone();
            match mutation {
                0 => wrong.fail_on_decode = false,
                1 => wrong.checkpoint_dir = None,
                2 => wrong.checkpoint.as_mut().unwrap().resume_latest = false,
                3 => wrong.checkpoint.as_mut().unwrap().interval_ms = Some(99),
                4 => wrong.graph_io.as_mut().unwrap().idle_after_ms = Some(99),
                5 => {
                    wrong
                        .graph_io
                        .as_mut()
                        .unwrap()
                        .sinks
                        .get_mut(&6)
                        .unwrap()
                        .skip_verify = true
                }
                6 => {
                    wrong
                        .graph_io
                        .as_mut()
                        .unwrap()
                        .sinks
                        .get_mut(&7)
                        .unwrap()
                        .kind = "log".into()
                }
                _ => {
                    wrong
                        .graph_io
                        .as_mut()
                        .unwrap()
                        .sources
                        .get_mut(&2)
                        .unwrap()
                        .kind = "mqtt".into()
                }
            }
            assert!(
                crate::validate_aligned_plan(&wrong, &plan).is_err(),
                "mutation {mutation}"
            );
        }
    }
}

#[test]
fn time_graph_iot_debounce_and_positive_ttl_restore() {
    for timed in [true, false] {
        let dir = Scratch::new();
        append(&dir.0, "a.ndjson", 3, 1);
        append(&dir.0, "b.ndjson", 7, 1);
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let http=sparrow_connectors::HttpCapture::start().await.unwrap();let store=store();store.put_allow("127.0.0.1",http.port()).unwrap();
            let mut config=configuration(&dir.0,[&http.url(),&http.url()],false);
            let mut node=json!({"id":4,"kind":"change_detect","iot":{"keys":["ts"],"fields":["v"],"ttl_micros":1000000,"max_keys":8,"invalid":"error","emit_first":true},"out":[5]});
            if timed {node["kind"]=json!("debounce");node["iot"]["ttl_micros"]=json!(0);node["iot"]["emit_first"]=json!(false);node["iot"]["timing"]=json!({"kind":"debounce","clock":"paused","quiet_micros":1000000,"max_wait_micros":2000000,"leading":false,"trailing":true,"reset_on_repeat":true});}
            config["graph"]["nodes"][3]=node;
            store.put_pipeline("graph",&parse(config),None).unwrap();let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();request_start(&store,"graph","test").unwrap();
            wait_cut(&sup,&store,&dir.0,|c|c.ingested==2).await;sup.kill_named("graph").await.unwrap();
            let count=if timed {0}else{4};assert_eq!(outputs(&http).len(),count);
            tokio::time::sleep(Duration::from_millis(1100)).await;
            if !timed {append(&dir.0,"a.ndjson",7,1);}
            request_start(&store,"graph","test").unwrap();
            if timed {wait_cut(&sup,&store,&dir.0,|c|c.outputs.values().all(|n|*n==2)).await;assert_eq!(outputs(&http).len(),2);assert!(outputs(&http).iter().all(|v|v["data"]["v"]==7));}
            else {wait_cut(&sup,&store,&dir.0,|c|c.ingested==3).await;assert_eq!(outputs(&http).len(),count,"downtime cannot expire TTL");}
            sup.stop_all().await;http.stop().await;
        });
    }
}

#[test]
fn time_graph_et_failed_current_reuses_recorded_future_skew_observation() {
    let dir = Scratch::new();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut config = configuration(&dir.0, [&http.url(), &http.url()], true);
        for index in [0, 1] {
            config["graph"]["nodes"][index]["max_future_skew_micros"] = json!(1000);
        }
        config["graph"]["nodes"][3]["window"]["max_future_skew_micros"] = json!(1000);
        store.put_pipeline("graph", &parse(config), None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "graph", "test").unwrap();
        wait_cut(&sup, &store, &dir.0, |c| c.sequence >= 1).await;
        sup.kill_named("graph").await.unwrap();
        let current = std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap();
        std::fs::create_dir(dir.0.join("checkpoints/CURRENT.tmp")).unwrap();
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            + 2_000_000;
        append(&dir.0, "a.ndjson", 99, future);
        request_start(&store, "graph", "test").unwrap();
        sup.converge_once().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let raw = std::fs::read(dir.0.join("checkpoints/TIME_PENDING")).unwrap();
                let logged: Value = serde_json::from_slice(&raw[36..]).unwrap();
                if logged["selected"] == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        sup.kill_named("graph").await.unwrap();
        assert_eq!(
            std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap(),
            current
        );
        std::fs::remove_dir(dir.0.join("checkpoints/CURRENT.tmp")).unwrap();
        tokio::time::sleep(Duration::from_millis(2100)).await;
        request_start(&store, "graph", "test").unwrap();
        let cut = wait_cut(&sup, &store, &dir.0, |c| c.ingested == 1).await;
        assert!(
            cut.sources[&1].progress.watermark.is_none(),
            "replay resampled current wall time"
        );
        assert!(
            snapshot(&dir.0).unwrap().windows[0].entries.is_empty(),
            "rejected future row became valid during downtime"
        );
        assert!(outputs(&http).is_empty());
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn time_graph_pt_multi_source_multi_sink_restores_pending_window() {
    let dir = Scratch::new();
    append(&dir.0, "a.ndjson", 3, 10);
    append(&dir.0, "b.ndjson", 7, 20);
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let a = sparrow_connectors::HttpCapture::start().await.unwrap();
        let b = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        for http in [&a, &b] {
            store.put_allow("127.0.0.1", http.port()).unwrap();
        }
        let spec = parse(configuration(&dir.0, [&a.url(), &b.url()], false));
        store.put_pipeline("graph", &spec, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "graph", "test").unwrap();
        wait_cut(&sup, &store, &dir.0, |c| c.ingested == 2).await;
        sup.kill_named("graph").await.unwrap();
        assert!(outputs(&a).is_empty());
        assert!(outputs(&b).is_empty());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        request_start(&store, "graph", "test").unwrap();
        wait_cut(&sup, &store, &dir.0, |c| {
            c.outputs.values().all(|v| *v == 2)
        })
        .await;
        sup.kill_named("graph").await.unwrap();
        assert_eq!(outputs(&a)[0]["data"]["total"], 10);
        assert_eq!(outputs(&a)[0]["data"], outputs(&b)[0]["data"]);
        assert_ne!(
            outputs(&a)[0]["id"],
            outputs(&b)[0]["id"],
            "Sink output namespaces must differ"
        );
        request_start(&store, "graph", "test").unwrap();
        let seq = GraphCut::unwrap(&snapshot(&dir.0).unwrap().source)
            .unwrap()
            .sequence;
        wait_cut(&sup, &store, &dir.0, |c| c.sequence > seq).await;
        assert_eq!(outputs(&a).len(), 1);
        assert_eq!(outputs(&b).len(), 1);
        sup.stop_all().await;
        a.stop().await;
        b.stop().await;
        assert_eq!(kernel.admitted_jobs(), 0);
        assert_eq!(kernel.live_tasks(), 0);
    });
}

#[test]
fn time_graph_et_slow_source_and_final_eof() {
    for sealed in [false, true] {
        let dir = Scratch::new();
        append(&dir.0, "a.ndjson", 2, 10);
        append(&dir.0, "a.ndjson", 3, 210);
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async {
            let a = sparrow_connectors::HttpCapture::start().await.unwrap();
            let b = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = store();
            for http in [&a, &b] {
                store.put_allow("127.0.0.1", http.port()).unwrap();
            }
            let mut config = configuration(&dir.0, [&a.url(), &b.url()], true);
            if sealed {
                for id in ["1", "2"] {
                    config["graph_io"]["sources"][id]["file_contract"] = json!("sealed");
                }
            }
            config["source"] = config["graph_io"]["sources"]["1"].clone();
            let spec = parse(config);
            store.put_pipeline("graph", &spec, None).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "graph", "test").unwrap();
            if sealed {
                wait_cut(&sup, &store, &dir.0, |c| {
                    c.sources.values().all(|s| s.progress.eof)
                })
                .await;
                assert_eq!(
                    outputs(&a)
                        .iter()
                        .map(|v| v["data"]["total"].as_i64().unwrap())
                        .collect::<Vec<_>>(),
                    vec![2, 3],
                    "EOF must close positive-lateness tail"
                );
            } else {
                let cut =
                    wait_cut(&sup, &store, &dir.0, |c| c.ingested == 2 && c.sequence >= 4).await;
                assert!(!cut.sources[&2].progress.idle);
                assert!(
                    outputs(&a).is_empty(),
                    "empty append-only source cannot imply EOF or idle"
                );
                append(&dir.0, "b.ndjson", 5, 10);
                append(&dir.0, "b.ndjson", 7, 210);
                wait_cut(&sup, &store, &dir.0, |c| c.ingested == 4).await;
                assert_eq!(outputs(&a).len(), 1);
                assert_eq!(outputs(&a)[0]["data"]["total"], 7);
            }
            sup.kill_named("graph").await.unwrap();
            let before = outputs(&a);
            let prior = GraphCut::unwrap(&snapshot(&dir.0).unwrap().source).unwrap();
            request_start(&store, "graph", "test").unwrap();
            wait_cut(&sup, &store, &dir.0, |c| c.sequence > prior.sequence).await;
            assert_eq!(
                outputs(&a),
                before,
                "restored EOF/watermark cannot republish committed rows"
            );
            sup.stop_all().await;
            a.stop().await;
            b.stop().await;
        });
    }
}

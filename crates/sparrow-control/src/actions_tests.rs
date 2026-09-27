//! Real Supervisor -> connector -> file/HTTP effects, not just helpers.
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn directory() -> Dir {
    let path = sparrow_connectors::ensure_default_data_root().join(format!(
        "actions-control-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(path.join("output")).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path.join("output"), std::fs::Permissions::from_mode(0o700)).unwrap();
    Dir(path)
}
fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("sensors",r#"{"fields":[{"name":"device","type":"utf8","nullable":false},{"name":"n","type":"uint64","nullable":false}]}"#).unwrap();
    store
}
fn parse(value: Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}
fn file(dir: &Dir) -> Value {
    json!({"kind":"file","file":{"directory":dir.0.join("output"),"segment_bytes":1024,"max_bytes":4096,"max_files":4,"row_bytes":1000,"sync_data":true}})
}
fn spec(dir: &Dir) -> Value {
    json!({"version":1,"stream":"sensors","sql":"SELECT concat(device, '-out') AS label, to_string(n) AS number FROM sensors",
    "source":{"kind":"file","path":dir.0.join("input.ndjson"),"file_contract":"sealed"},"sink":file(dir),"recovery":"restart_fresh","fail_on_decode":true})
}
async fn finish(sup: &Arc<Supervisor>, store: &Arc<Store>) -> String {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            sup.converge_once().await.unwrap();
            let actual = store.actual("actions").unwrap();
            if matches!(actual.status.as_str(), "failed" | "completed") {
                return actual.status.to_string();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("action completion deadline")
}
#[test]
fn actions_control_admission_schema_and_recovery_fail_closed() {
    let dir = directory();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let store = store();
    let mut value = spec(&dir);
    value["sink"]["action"] = json!({"body":{"name":{"$field":"label"}}});
    let good = parse(value.clone());
    let plan = crate::bind_plan_with_store(&store, &good, "actions", 1).unwrap();
    let schema = crate::stream_to_schema(&store.get_stream("sensors").unwrap()).unwrap();
    let policy = sparrow_connectors::TargetPolicy::allow("127.0.0.1", 99);
    let secrets = sparrow_connectors::MapSecretResolver::empty();
    crate::validate::validate_io_with_plan(&good, &schema, &plan, &secrets, &policy, None).unwrap();
    crate::validate_aligned_plan(&good, &plan).unwrap();
    for mutation in 0..11 {
        let mut wrong = value.clone();
        match mutation {
            0 => wrong["sink"]["action"]["body"] = json!({"$field":"device"}), // pre-Project name must not resolve
            1 => wrong["sink"]["action"]["version"] = json!(2),
            2 => wrong["sink"]["action"]["query"] = json!({"q":["x"]}),
            3 => wrong["sink"]["file"]["row_bytes"] = json!(1024),
            4 => wrong["checkpoint_dir"] = json!(dir.0.join("checkpoints")),
            5 => wrong["recovery"] = json!("aligned"),
            6 => wrong["sink"]["url"] = json!("http://127.0.0.1:99/"),
            7 => wrong["sink"]["action"]["topic"] = json!(["out/"]),
            8 => wrong["sink"]["qos"] = json!(1),
            9 => wrong["sink"]["client_id"] = json!("ignored"),
            _ => wrong["sink"]["clean_session"] = json!(false),
        }
        let wrong = parse(wrong);
        let io =
            crate::validate::validate_io_with_plan(&wrong, &schema, &plan, &secrets, &policy, None);
        assert!(
            io.is_err() || crate::validate_aligned_plan(&wrong, &plan).is_err(),
            "mutation {mutation}"
        );
    }
    let mut http = value;
    http["sink"] = json!({"kind":"http","url":"http://127.0.0.1:99/?q=fixed","action":{"query":{"q":["row"]}}});
    assert!(crate::validate::validate_io_with_plan(
        &parse(http),
        &schema,
        &plan,
        &secrets,
        &policy,
        None
    )
    .is_err());
    assert!(
        !dir.0.join("output/FORMAT").exists(),
        "validation must not create output"
    );
}

#[test]
fn actions_control_shipped_templates_are_bindable() {
    let dir = directory();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let store = store();
    store
        .put_stream(
            "actions",
            include_str!("../../../deploy/stream-actions.json"),
        )
        .unwrap();
    let schema = crate::stream_to_schema(&store.get_stream("actions").unwrap()).unwrap();
    let policy =
        sparrow_connectors::TargetPolicy::allow("127.0.0.1", 9081).with_allow("127.0.0.1", 1883);
    for template in [
        include_bytes!("../../../deploy/pipeline-actions-http.json").as_slice(),
        include_bytes!("../../../deploy/pipeline-actions-mqtt.json").as_slice(),
        include_bytes!("../../../deploy/pipeline-actions-file.json").as_slice(),
    ] {
        let mut spec = PipelineSpec::from_json(template).unwrap();
        spec.source.path = Some(dir.0.join("input.ndjson").to_string_lossy().into());
        if let Some(file) = &mut spec.sink.file {
            file.directory = dir.0.join("output").to_string_lossy().into();
        }
        let plan = crate::bind_plan_with_store(&store, &spec, "template", 1).unwrap();
        crate::validate_aligned_plan(&spec, &plan).unwrap();
        crate::validate::validate_io_with_plan(
            &spec,
            &schema,
            &plan,
            &sparrow_connectors::MapSecretResolver::empty(),
            &policy,
            None,
        )
        .unwrap();
    }
}
#[test]
fn actions_control_file_output_functions_and_no_overwrite_on_fresh_restart() {
    let dir = directory();
    std::fs::write(
        dir.0.join("input.ndjson"),
        b"{\"device\":\"a\",\"n\":18446744073709551615}\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let store = store();
        let mut value = spec(&dir);
        value["sink"]["action"] =
            json!({"body":{"mapped":{"$field":"label"},"number":{"$field":"number"}}});
        let spec = parse(value);
        store.put_pipeline("actions", &spec, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "actions", "test").unwrap();
        assert_eq!(finish(&sup, &store).await, "completed");
        let first = dir.0.join("output/part-00000000000000000001.ndjson");
        let bytes = std::fs::read(&first).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap(),
            json!({"mapped":"a-out","number":"18446744073709551615"})
        );
        // New explicit revision starts fresh; data is repeated in a NEW segment.
        let previous = store.get_pipeline("actions").unwrap();
        store
            .put_pipeline("actions", &spec, Some(&previous.etag))
            .unwrap();
        request_start(&store, "actions", "test").unwrap();
        assert_eq!(finish(&sup, &store).await, "completed");
        assert_eq!(std::fs::read(first).unwrap(), bytes);
        assert_eq!(
            std::fs::read(dir.0.join("output/part-00000000000000000002.ndjson")).unwrap(),
            bytes
        );
    });
}
#[test]
fn actions_control_file_open_failure_is_job_failure_without_input() {
    let dir = directory();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    std::fs::write(dir.0.join("output/foreign"), b"preserve").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let store = store();
        let mut value = spec(&dir);
        value["source"]["file_contract"] = json!("append_only");
        store.put_pipeline("actions", &parse(value), None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "actions", "test").unwrap();
        assert_eq!(finish(&sup, &store).await, "failed");
        assert_eq!(
            std::fs::read(dir.0.join("output/foreign")).unwrap(),
            b"preserve"
        );
        assert!(!dir.0.join("output/FORMAT").exists());
    });
}
#[test]
fn actions_control_dag_required_optional_failure_and_multiple_outputs() {
    for mode in [
        "success",
        "required_failure",
        "optional_failure",
        "idle_failure",
    ] {
        let dir = directory();
        let input = if mode == "idle_failure" {
            ""
        } else {
            "{\"device\":\"a\",\"n\":7}\n"
        };
        std::fs::write(dir.0.join("input.ndjson"), input).unwrap();
        if mode != "success" {
            std::fs::write(dir.0.join("output/foreign"), b"preserve").unwrap();
        }
        let kernel = Arc::new(crate::host_kernel().unwrap());
        kernel.block_on(async{
            let http=sparrow_connectors::HttpCapture::start().await.unwrap();let store=store();store.put_allow("127.0.0.1",http.port()).unwrap();
            let source=json!({"kind":"file","path":dir.0.join("input.ndjson"),"file_contract":if mode=="idle_failure"{"append_only"}else{"sealed"}});
            let sink=json!({"kind":"http","url":http.url(),"action":{"body":{"device":{"$field":"device"},"n":{"$field":"n"}}}});
            let mut branch=json!({"id":2,"kind":"branch","out":[3,4]});if mode=="optional_failure" {branch["best_effort"]=json!([4]);}
            let value=json!({"version":1,"stream":"sensors","source":source,"sink":sink,"recovery":"restart_fresh",
                "graph_io":{"sources":{"1":source},"sinks":{"3":sink,"4":file(&dir)}},
                "graph":{"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
                    {"id":1,"kind":"memory_source","table":"sensors","out":[2]},branch,
                    {"id":3,"kind":"capture_sink","name":"http"},{"id":4,"kind":if mode=="optional_failure"{"best_effort_sink"}else{"capture_sink"},"name":"file"}]}});
            store.put_pipeline("actions",&parse(value),None).unwrap();let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();request_start(&store,"actions","test").unwrap();
            assert_eq!(finish(&sup,&store).await,if matches!(mode,"success"|"optional_failure"){"completed"}else{"failed"},"{mode}");
            if matches!(mode,"success"|"optional_failure") {assert_eq!(http.bodies().len(),1,"{mode}");assert_eq!(serde_json::from_slice::<Value>(&http.bodies()[0]).unwrap(),json!([{"device":"a","n":7}]));}
            if mode=="success" {assert_eq!(serde_json::from_slice::<Value>(&std::fs::read(dir.0.join("output/part-00000000000000000001.ndjson")).unwrap()).unwrap(),json!({"device":"a","n":7}));}
        });
    }
}

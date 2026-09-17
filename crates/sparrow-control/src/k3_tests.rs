//! Real File -> graph -> independent HTTP sinks, through the normal catalog
//! and Supervisor. Assertions use raw input/output values, not the evaluator.
use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

struct Scratch(std::path::PathBuf);
impl Scratch {
    fn new() -> Self {
        let root = sparrow_connectors::ensure_default_data_root().join(format!(
            "sparrow-k3-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn output(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|b| serde_json::from_slice::<Vec<Value>>(b).unwrap())
        .collect()
}
async fn wait_rows(
    http: &sparrow_connectors::HttpCapture,
    n: usize,
    sup: &Arc<Supervisor>,
    store: &Arc<Store>,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while output(http).len() < n {
            sup.converge_once().await.unwrap();
            assert_ne!(
                store.actual("graph").unwrap().status,
                "failed",
                "{:?}",
                store.actual("graph").unwrap()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("graph output deadline");
}
fn append(file: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(file).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn k3_file_multi_source_multi_sink_checkpoint_restore_and_identity_refusal() {
    let scratch = Scratch::new();
    let a = scratch.0.join("a.ndjson");
    let b = scratch.0.join("b.ndjson");
    std::fs::write(&a, b"{\"v\":1}\n{\"v\":2}\n").unwrap();
    std::fs::write(&b, b"{\"v\":10}\n{\"v\":20}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async{
        let first=sparrow_connectors::HttpCapture::start().await.unwrap();let second=sparrow_connectors::HttpCapture::start().await.unwrap();
        let store=Arc::new(Store::open(&scratch.0.join("catalog.db")).unwrap());
        store.put_stream("s",r#"{"fields":[{"name":"v","type":"int64","nullable":false}]}"#).unwrap();
        store.put_allow("127.0.0.1",first.port()).unwrap();store.put_allow("127.0.0.1",second.port()).unwrap();
        let source=json!({"kind":"file","path":a,"file_contract":"append_only","inbox_capacity":4});
        let other=json!({"kind":"file","path":b,"file_contract":"append_only","inbox_capacity":4});
        let sink=json!({"kind":"http","url":first.url(),"batch_rows":2,"linger_ms":1,"outbox_capacity":4});
        let sink2=json!({"kind":"http","url":second.url(),"batch_rows":2,"linger_ms":1,"outbox_capacity":4});
        let spec=PipelineSpec::from_json(&serde_json::to_vec(&json!({"stream":"s","source":source,"sink":sink,
            "recovery":"aligned","checkpoint_dir":scratch.0.join("checkpoints"),"checkpoint":{"timeout_ms":2000,"resume_latest":true},
            "graph_io":{"sources":{"1":source,"2":other},"sinks":{"5":sink,"6":sink2}},
            "graph":{"version":1,"pipeline_id":44,"revision_id":1,"nodes":[
                {"id":1,"kind":"memory_source","table":"s","out":[3]},{"id":2,"kind":"memory_source","table":"s","out":[3]},
                {"id":3,"kind":"union_all","out":[4]},{"id":4,"kind":"branch","out":[5,6]},
                {"id":5,"kind":"capture_sink","name":"a"},{"id":6,"kind":"capture_sink","name":"b"}]}})).unwrap()).unwrap();
        store.put_pipeline("graph",&spec,None).unwrap();request_start(&store,"graph","test").unwrap();
        let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();sup.converge_once().await.unwrap();
        wait_rows(&first,4,&sup,&store).await;wait_rows(&second,4,&sup,&store).await;
        let checkpoint=sup.checkpoint_named("graph").await.unwrap();
        let mut initial:Vec<_>=output(&first).into_iter().map(|v|v["v"].as_i64().unwrap()).collect();initial.sort();assert_eq!(initial,vec![1,2,10,20]);
        append(&a,b"{\"v\":3}\n");append(&b,b"{\"v\":30}\n");
        wait_rows(&first,6,&sup,&store).await;wait_rows(&second,6,&sup,&store).await;
        sup.kill_named("graph").await.unwrap();request_start(&store,"graph","test").unwrap();sup.converge_once().await.unwrap();
        wait_rows(&first,8,&sup,&store).await;wait_rows(&second,8,&sup,&store).await;
        assert_eq!(sup.checkpoint_snapshot("graph").unwrap().unwrap().1.snapshot().restored_from,Some(checkpoint));
        for http in [&first,&second]{let mut replay:Vec<_>=output(http)[6..].iter().map(|v|v["v"].as_i64().unwrap()).collect();replay.sort();assert_eq!(replay,vec![3,30]);}
        sup.stop_all().await;
        std::fs::write(&b,b"{\"v\":99}\n").unwrap();request_start(&store,"graph","test").unwrap();sup.converge_once().await.unwrap();
        assert_ne!(store.actual("graph").unwrap().status,"running","replaced second source must refuse entire restore");
        sup.stop_all().await;assert_eq!(kernel.admitted_jobs(),0);assert_eq!(kernel.live_tasks(),0);
        first.stop().await;second.stop().await;
    });
}

#[test]
fn k3_file_decode_errors_have_typed_bounded_side_output() {
    let scratch = Scratch::new();
    let file = scratch.0.join("input.ndjson");
    std::fs::write(&file, b"{\"v\":1}\nbad-json\n{\"v\":2}\n").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async{
        let main=sparrow_connectors::HttpCapture::start().await.unwrap();let errors=sparrow_connectors::HttpCapture::start().await.unwrap();
        let store=Arc::new(Store::open_memory().unwrap());store.put_stream("s",r#"{"fields":[{"name":"v","type":"int64","nullable":false}]}"#).unwrap();
        for port in [main.port(),errors.port()]{store.put_allow("127.0.0.1",port).unwrap();}
        let source=json!({"kind":"file","path":file,"file_contract":"sealed"});let sink=json!({"kind":"http","url":main.url()});let side=json!({"kind":"http","url":errors.url()});
        let spec=PipelineSpec::from_json(&serde_json::to_vec(&json!({"stream":"s","source":source,"sink":sink,
            "graph_io":{"sources":{"1":source},"sinks":{"2":sink,"3":side}},
            "graph":{"version":1,"pipeline_id":45,"revision_id":1,"nodes":[{"id":1,"kind":"memory_source","table":"s","out":[2,3],"side_output":{"kind":"decode_error","to":3,"full":"backpressure"}},{"id":2,"kind":"capture_sink","name":"out"},{"id":3,"kind":"capture_sink","name":"errors"}]}})).unwrap()).unwrap();
        store.put_pipeline("graph",&spec,None).unwrap();request_start(&store,"graph","test").unwrap();let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();sup.converge_once().await.unwrap();
        wait_rows(&main,2,&sup,&store).await;wait_rows(&errors,1,&sup,&store).await;
        assert_eq!(output(&errors),vec![json!({"source_operator":1,"error_code":"CodecViolation"})]);
        assert_eq!(kernel.metrics.snapshot().graph_side_rows,1);sup.stop_all().await;main.stop().await;errors.stop().await;
    });
}

use crate::{request_start, PipelineSpec, Store, Supervisor};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

struct Scratch(std::path::PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn scratch() -> Scratch {
    let dir = sparrow_connectors::ensure_default_data_root().join(format!(
        "paused-time-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    Scratch(dir)
}
fn configuration(dir: &std::path::Path, url: &str, kind: &str) -> Value {
    let timing = if kind == "hold_for" {
        json!({"kind":"hold_for","clock":"paused","duration_micros":1000000})
    } else {
        json!({"kind":"debounce","clock":"paused","quiet_micros":200000,"max_wait_micros":1000000,
            "leading":false,"trailing":true,"reset_on_repeat":true})
    };
    json!({"version":1,"stream":"sensors","source":{"kind":"file","path":dir.join("input.ndjson"),"file_contract":"append_only"},
        "sink":{"kind":"http","url":url,"batch_rows":1,"linger_ms":0},"fail_on_decode":true,"recovery":"aligned",
        "checkpoint_dir":dir.join("checkpoints"),"checkpoint":{"interval_ms":100,"timeout_ms":5000,"resume_latest":true},
        "graph":{"version":1,"pipeline_id":715,"revision_id":1,"nodes":[
            {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
            {"id":2,"kind":kind,"iot":{"keys":["device_id"],"fields":["active"],"emit_first":false,
                "ttl_micros":0,"max_keys":16,"invalid":"ignore","timing":timing},"out":[3]},
            {"id":3,"kind":"capture_sink","name":"http"}]}})
}
fn parse(value: &Value) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(value).unwrap()).unwrap()
}
fn store() -> Arc<Store> {
    let store = Arc::new(Store::open_memory().unwrap());
    store.put_stream("sensors",r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"active","type":"bool","nullable":true}]}"#).unwrap();
    store
}
fn outputs(http: &sparrow_connectors::HttpCapture) -> Vec<Value> {
    http.bodies()
        .iter()
        .flat_map(|b| serde_json::from_slice::<Vec<Value>>(b).unwrap())
        .collect()
}
fn snapshot(dir: &std::path::Path) -> sparrow_runtime::PipelineSnapshot {
    sparrow_runtime::CheckpointStore::open_readonly(dir.join("checkpoints"))
        .unwrap()
        .recover_pipeline_required()
        .unwrap()
}
async fn wait_cut(sup: &Arc<Supervisor>, store: &Store, dir: &std::path::Path, rows: u64) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            sup.converge_once().await.unwrap();
            assert_ne!(store.actual("time").unwrap().status, "failed");
            if dir.join("checkpoints/CURRENT").is_file() && snapshot(dir).ingested_rows >= rows {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_output(
    sup: &Arc<Supervisor>,
    store: &Store,
    http: &sparrow_connectors::HttpCapture,
    n: usize,
) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while outputs(http).len() < n {
            sup.converge_once().await.unwrap();
            assert_ne!(store.actual("time").unwrap().status, "failed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[test]
fn paused_time_control_profile_rejects_unsupported_combinations() {
    let dir = scratch();
    let store = store();
    let value = configuration(&dir.0, "http://127.0.0.1:1/ingest", "hold_for");
    let spec = parse(&value);
    let plan = crate::bind_plan_with_store(&store, &spec, "time", 1).unwrap();
    crate::validate_aligned_plan(&spec, &plan).unwrap();
    assert_eq!(
        crate::effective_guarantees_with_plan(&spec, &plan)["iot"]["snapshot_version"],
        14
    );
    for mutation in 0..7 {
        let mut spec = spec.clone();
        match mutation {
            0 => spec.recovery = "restart_fresh".into(),
            1 => spec.checkpoint.as_mut().unwrap().resume_latest = false,
            2 => spec.checkpoint.as_mut().unwrap().interval_ms = None,
            3 => spec.fail_on_decode = false,
            4 => spec.source.file_contract = Some("sealed".into()),
            5 => spec.sink.kind = "log".into(),
            _ => spec.checkpoint.as_mut().unwrap().interval_ms = Some(10000),
        };
        assert!(
            crate::validate_aligned_plan(&spec, &plan).is_err(),
            "mutation {mutation}"
        );
    }
}
#[test]
fn paused_time_file_hold_survives_downtime_without_new_input() {
    let dir = scratch();
    std::fs::write(
        dir.0.join("input.ndjson"),
        b"{\"device_id\":\"a\",\"active\":true}\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&configuration(&dir.0, &http.url(), "hold_for"));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_cut(&sup, &store, &dir.0, 1).await;
        sup.kill_named("time").await.unwrap();
        assert!(outputs(&http).is_empty());
        let stopped = snapshot(&dir.0);
        let cut = sparrow_runtime::processing_cut::ProcessingCut::unwrap(&stopped.source).unwrap();
        let sparrow_model::Scalar::Int64(deadline) = stopped.iot[0].entries[0].values[1] else {
            panic!("hold_for snapshot must contain an Int64 deadline");
        };
        assert!(deadline - cut.micros > 500000, "fixture stopped too late");
        tokio::time::sleep(Duration::from_millis(1100)).await;
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            outputs(&http).is_empty(),
            "downtime must not advance logical time"
        );
        wait_output(&sup, &store, &http, 1).await;
        assert_eq!(
            outputs(&http)[0]["data"],
            json!({"device_id":"a","active":true})
        );
        assert!(outputs(&http)[0]["id"].is_string());
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(outputs(&http).len(), 1);
        sup.stop_all().await;
        http.stop().await;
    });
}
#[test]
fn paused_time_file_pending_timer_replays_identical_output_after_blocked_sink() {
    let dir = scratch();
    std::fs::write(
        dir.0.join("input.ndjson"),
        b"{\"device_id\":\"a\",\"active\":true}\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = HeldHttp::start().await;
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&configuration(&dir.0, &http.url(), "debounce"));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_cut(&sup, &store, &dir.0, 1).await;
        http.wait(1).await;
        let old = snapshot(&dir.0);
        let current = std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap(),
            current
        );
        let first = http.outputs()[0].clone();
        sup.kill_named("time").await.unwrap();
        assert_eq!(snapshot(&dir.0).checkpoint_id, old.checkpoint_id);
        http.release();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        http.wait(2).await;
        assert_eq!(
            http.outputs()[1],
            first,
            "replayed timer must keep output identity and payload"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(snapshot(&dir.0).checkpoint_id > old.checkpoint_id);
        sup.stop_all().await;
        assert_eq!(http.outputs().len(), 2);
        http.stop().await;
    });
}

#[test]
fn paused_time_file_current_failure_keeps_pending_decision_for_replay() {
    let dir = scratch();
    std::fs::write(
        dir.0.join("input.ndjson"),
        b"{\"device_id\":\"a\",\"active\":true}\n",
    )
    .unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = HeldHttp::start().await;
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&configuration(&dir.0, &http.url(), "debounce"));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_cut(&sup, &store, &dir.0, 1).await;
        http.wait(1).await;
        let current = std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap();
        let pending = std::fs::read(dir.0.join("checkpoints/TIME_PENDING")).unwrap();
        std::fs::create_dir(dir.0.join("checkpoints/CURRENT.tmp")).unwrap();
        http.release();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                sup.converge_once().await.unwrap();
                if store.actual("time").unwrap().status == "failed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap(),
            current
        );
        assert_eq!(
            std::fs::read(dir.0.join("checkpoints/TIME_PENDING")).unwrap(),
            pending
        );
        sup.kill_named("time").await.unwrap();
        std::fs::remove_dir(dir.0.join("checkpoints/CURRENT.tmp")).unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        http.wait(2).await;
        assert_eq!(http.outputs()[0], http.outputs()[1]);
        sup.stop_all().await;
        http.stop().await;
    });
}

#[test]
fn paused_time_file_missing_log_refuses_resume_without_advancing_current() {
    let dir = scratch();
    std::fs::write(dir.0.join("input.ndjson"), b"").unwrap();
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&configuration(&dir.0, &http.url(), "hold_for"));
        store.put_pipeline("time", &spec, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_cut(&sup, &store, &dir.0, 0).await;
        sup.kill_named("time").await.unwrap();
        let current = std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap();
        std::fs::remove_file(dir.0.join("checkpoints/TIME_PENDING")).unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        let actual = store.actual("time").unwrap();
        assert_eq!(actual.status, "failed");
        assert!(actual
            .last_error
            .unwrap()
            .contains("TIME_PENDING is missing"));
        assert_eq!(
            std::fs::read(dir.0.join("checkpoints/CURRENT")).unwrap(),
            current
        );
        assert!(outputs(&http).is_empty());
        sup.stop_all().await;
        http.stop().await;
    });
}

#[cfg(feature = "jetstream")]
#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn paused_time_jetstream_hold_downtime_and_committed_broker_cut() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture=sparrow_testkit::nats::NatsSandbox::start().await;let context=fixture.provision().await;
        let http=sparrow_connectors::HttpCapture::start().await.unwrap();let store=store();
        store.put_allow("127.0.0.1",fixture.port).unwrap();store.put_allow("127.0.0.1",http.port()).unwrap();
        let mut value=configuration(&fixture.root,&http.url(),"hold_for");
        value["source"]=json!({"kind":"jetstream","jetstream":{"servers":[fixture.url()],"namespace":"time_test","stream":"INPUT",
            "consumer":"paused","ownership_bucket":"OWNERS","max_pending":16,"pending_bytes":262144,"pull_messages":4,"pull_bytes":73728}});
        value["delivery"]=json!("checkpointed_at_least_once");let spec=parse(&value);
        store.put_pipeline("time",&spec,None).unwrap();request_start(&store,"time","test").unwrap();
        let sup=Supervisor::new(store.clone(),kernel.clone(),false,None).unwrap();sup.converge_once().await.unwrap();
        wait_cut(&sup,&store,&fixture.root,0).await;
        context.publish("input.rows",b"{\"device_id\":\"a\",\"active\":true}".to_vec().into()).await.unwrap().await.unwrap();
        wait_cut(&sup,&store,&fixture.root,1).await;
        let kv=context.get_key_value("OWNERS").await.unwrap();let entry=kv.entry("INPUT.paused").await.unwrap().unwrap();
        let nonce=entry.value[36..].iter().map(|b|format!("{b:02x}")).collect::<String>();
        let mut consumer:sparrow_testkit::nats::jetstream::consumer::PullConsumer=context.get_stream("INPUT").await.unwrap().get_consumer(&format!("paused_{nonce}")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),async {loop {let info=consumer.info().await.unwrap();if info.ack_floor.stream_sequence==1 && info.num_ack_pending==0 {break;}
            tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
        sup.kill_named("time").await.unwrap();assert!(outputs(&http).is_empty());
        let stopped=snapshot(&fixture.root);assert_eq!(stopped.source.identity.kind,sparrow_runtime::processing_cut::JETSTREAM_KIND);
        tokio::time::sleep(Duration::from_millis(1100)).await;request_start(&store,"time","test").unwrap();sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;assert!(outputs(&http).is_empty());wait_output(&sup,&store,&http,1).await;
        assert_eq!(outputs(&http)[0]["data"],json!({"device_id":"a","active":true}));sup.stop_all().await;http.stop().await;
    });
}

/// Captures bytes BEFORE acknowledging HTTP. The production demo capture
/// delays before storing bytes, so it cannot prove an uncommitted output cut.
struct HeldHttp {
    port: u16,
    bodies: Arc<std::sync::Mutex<Vec<Value>>>,
    held: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<tokio::sync::Notify>,
    cancel: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl HeldHttp {
    async fn start() -> Self {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let held = Arc::new(AtomicBool::new(true));
        let release = Arc::new(tokio::sync::Notify::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let (b, h, r, c) = (
            bodies.clone(),
            held.clone(),
            release.clone(),
            cancel.clone(),
        );
        let task = tokio::spawn(async move {
            let mut workers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=c.cancelled()=>break,
                    accepted=listener.accept()=>{
                        let (mut socket,_)=accepted.unwrap();let (b,h,r,c)=(b.clone(),h.clone(),r.clone(),c.clone());
                        workers.spawn(async move {
                            let mut bytes=Vec::new();let mut chunk=[0;1024];
                            let header=loop {
                                let n=tokio::select! {_=c.cancelled()=>return,n=socket.read(&mut chunk)=>n.unwrap()};
                                if n==0{return;}bytes.extend_from_slice(&chunk[..n]);assert!(bytes.len()<128*1024);
                                if let Some(end)=bytes.windows(4).position(|w|w==b"\r\n\r\n"){break end+4;}
                            };
                            let len=std::str::from_utf8(&bytes[..header]).unwrap().lines().find_map(|line|
                                line.to_ascii_lowercase().strip_prefix("content-length:").map(|n|n.trim().parse::<usize>().unwrap())).unwrap();
                            assert!(len<=64*1024);
                            while bytes.len()<header+len {let n=tokio::select! {_=c.cancelled()=>return,n=socket.read(&mut chunk)=>n.unwrap()};if n==0{return;}bytes.extend_from_slice(&chunk[..n]);}
                            b.lock().unwrap().extend(serde_json::from_slice::<Vec<Value>>(&bytes[header..header+len]).unwrap());
                            let notified=r.notified();tokio::pin!(notified);notified.as_mut().enable();
                            if h.load(Ordering::SeqCst) {tokio::select! {_=c.cancelled()=>return,_=&mut notified=>{}}}
                            let _=socket.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n").await;
                        });
                    }
                    joined=workers.join_next(),if !workers.is_empty()=>{joined.unwrap().unwrap();},
                }
            }
            while let Some(joined) = workers.join_next().await {
                joined.unwrap();
            }
        });
        Self {
            port,
            bodies,
            held,
            release,
            cancel,
            task,
        }
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/ingest", self.port)
    }
    fn outputs(&self) -> Vec<Value> {
        self.bodies.lock().unwrap().clone()
    }
    fn release(&self) {
        self.held.store(false, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_waiters();
    }
    async fn wait(&self, n: usize) {
        tokio::time::timeout(Duration::from_secs(6), async {
            while self.outputs().len() < n {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn stop(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}

//! Independent source/output oracle against a real isolated JetStream server,
//! real Supervisor, checkpoint files, Kernel and HTTP sink.
use crate::{request_start, PipelineSpec, Store, Supervisor};
use sparrow_testkit::nats::NatsSandbox;
use std::{sync::Arc, time::Duration};

#[test]
fn capacity_idle_tuning_admission_and_checkpoint_semantics() {
    use serde_json::json;
    let store=Store::open_memory().unwrap();store.put_stream("sensors",r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}"#).unwrap();
    let base=json!({"stream":"sensors","sql":"SELECT device_id, v FROM sensors",
        "source":{"kind":"jetstream","jetstream":{"servers":["nats://127.0.0.1:4222"],"namespace":"capacity","stream":"INPUT","consumer":"case","ownership_bucket":"OWNERS"}},
        "sink":{"kind":"http","url":"http://127.0.0.1:9/unused"},"recovery":"aligned","delivery":"checkpointed_at_least_once",
        "checkpoint_dir":"/tmp/sparrow/capacity-unused","checkpoint":{"interval_ms":1000,"timeout_ms":5000,"resume_latest":true}});
    let parse=|value:&serde_json::Value|PipelineSpec::from_json(&serde_json::to_vec(value).unwrap());
    let original=parse(&base).unwrap();assert!(serde_json::to_value(&original).unwrap()["source"]["jetstream"].get("idle_backoff_max_ms").is_none());
    let old_plan=crate::bind_plan_with_store(&store,&original,"capacity",1).unwrap();
    for cap in [5u64,7,20,250] {
        let mut value=base.clone();value["source"]["jetstream"]["idle_backoff_max_ms"]=json!(cap);let spec=parse(&value).unwrap();
        let plan=crate::bind_plan_with_store(&store,&spec,"capacity",1).unwrap();crate::validate_aligned_plan(&spec,&plan).unwrap();assert_eq!(plan,old_plan,"operational idle tuning must not change checkpoint semantics");
        let effective=crate::validate::effective_guarantees_with_plan(&spec,&plan);
        assert_eq!(effective["jetstream_execution"]["idle_backoff_ms"]["maximum"],cap);
        assert_eq!(effective["jetstream_execution"]["idle_steady_pulls_per_second_max"].as_u64(),Some(1000u64.div_ceil(cap)));
    }
    for cap in [0,4,251] {let mut value=base.clone();value["source"]["jetstream"]["idle_backoff_max_ms"]=json!(cap);assert!(parse(&value).is_err());}
    let mut timed=base;timed.as_object_mut().unwrap().remove("sql");timed["source"]["jetstream"]["idle_backoff_max_ms"]=json!(20);
    timed["graph"]=json!({"version":1,"pipeline_id":1,"revision_id":1,"nodes":[
        {"id":1,"kind":"memory_source","table":"sensors","out":[2]},
        {"id":2,"kind":"debounce","iot":{"keys":["device_id"],"fields":["v"],"emit_first":false,"ttl_micros":0,"max_keys":16,"invalid":"error",
            "timing":{"kind":"debounce","clock":"paused","quiet_micros":200000,"max_wait_micros":1000000,"leading":false,"trailing":true,"reset_on_repeat":true}},"out":[3]},
        {"id":3,"kind":"capture_sink","name":"out"}]});
    let spec=parse(&timed).unwrap();let plan=crate::bind_plan_with_store(&store,&spec,"capacity",1).unwrap();
    assert!(crate::validate_aligned_plan(&spec,&plan).unwrap_err().message.contains("regular JetStream actor"));
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_stable_requires_new_durable_progress_and_restore_refuses_semantic_or_directory_forks() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let original = spec(
            &fixture,
            &http.url(),
            "identity",
            "SELECT device_id, v FROM sensors",
        );
        store.put_pipeline("test", &original, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let mut sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        Arc::get_mut(&mut sup).unwrap().stable_run = Duration::ZERO;
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        let actual = store.actual("test").unwrap();
        store
            .set_actual(
                "test",
                "failed",
                actual.revision,
                actual.attempt_id,
                Some("prior failure fixture"),
            )
            .unwrap();
        store
            .set_actual("test", "running", actual.revision, actual.attempt_id, None)
            .unwrap();
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("test").unwrap().consecutive_failures, 1);
        checkpoint_when_idle(&sup).await;
        sup.converge_once().await.unwrap();
        assert_eq!(
            store.actual("test").unwrap().consecutive_failures,
            1,
            "empty checkpoints are not recovery progress"
        );
        NatsSandbox::publish(&context, 1, 3).await;
        wait_outputs(&http, 3, &sup, &store).await;
        assert_eq!(
            store.actual("test").unwrap().consecutive_failures,
            1,
            "HTTP success alone must not reset failures"
        );
        checkpoint_when_idle(&sup).await;
        wait_acked(&context, "identity", 3).await;
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("test").unwrap().consecutive_failures, 0);
        sup.kill_named("test").await.unwrap();
        let mut changed = original.clone();
        changed.sql = Some("SELECT device_id, v + 1 AS v FROM sensors".into());
        let etag = store.get_pipeline("test").unwrap().etag;
        store.put_pipeline("test", &changed, Some(&etag)).unwrap();
        request_start(&store, "test", "test").unwrap();
        sup.converge_once().await.unwrap();
        assert!(store
            .actual("test")
            .unwrap()
            .last_error
            .unwrap()
            .contains("semantic changes"));
        let old = std::path::Path::new(original.checkpoint_dir.as_ref().unwrap());
        let moved = fixture.root.join("moved");
        std::fs::rename(old, &moved).unwrap();
        let current = std::fs::read(moved.join("CURRENT")).unwrap();
        let mut changed = original.clone();
        changed.checkpoint_dir = Some(moved.to_string_lossy().into_owned());
        let etag = store.get_pipeline("test").unwrap().etag;
        store.put_pipeline("test", &changed, Some(&etag)).unwrap();
        request_start(&store, "test", "test").unwrap();
        sup.converge_once().await.unwrap();
        assert!(store
            .actual("test")
            .unwrap()
            .last_error
            .unwrap()
            .contains("another checkpoint owner"));
        assert_eq!(std::fs::read(moved.join("CURRENT")).unwrap(), current);
        assert_eq!(outputs(&http).len(), 3);
        http.stop().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

fn spec(fixture: &NatsSandbox, url: &str, name: &str, sql: &str) -> PipelineSpec {
    PipelineSpec::from_json(&serde_json::to_vec(&serde_json::json!({
        "stream":"sensors","sql":sql,"source":{"kind":"jetstream","jetstream":{
            "servers":[fixture.url()],"namespace":"test_account","stream":"INPUT","consumer":name,
            "ownership_bucket":"OWNERS","max_pending":16,"pull_messages":4,"pending_bytes":262144,"pull_bytes":73728}},
        "sink":{"kind":"http","url":url,"batch_rows":32,"linger_ms":10},"recovery":"aligned","delivery":"checkpointed_at_least_once",
        "checkpoint_dir":fixture.root.join(format!("checkpoint-{name}")),
        "checkpoint":{"interval_ms":10000,"timeout_ms":2000,"resume_latest":true}
    })).unwrap()).unwrap()
}
fn catalog(fixture: &NatsSandbox, http: &sparrow_connectors::HttpCapture) -> Arc<Store> {
    let store = Arc::new(Store::open(&fixture.root.join("catalog.db")).unwrap());
    store.put_stream("sensors",r#"{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}"#).unwrap();
    store.put_allow("127.0.0.1", fixture.port).unwrap();
    store.put_allow("127.0.0.1", http.port()).unwrap();
    store
}
fn outputs(http: &sparrow_connectors::HttpCapture) -> Vec<serde_json::Value> {
    http.bodies()
        .iter()
        .flat_map(|b| serde_json::from_slice::<Vec<serde_json::Value>>(b).unwrap())
        .collect()
}
async fn wait_outputs(
    http: &sparrow_connectors::HttpCapture,
    n: usize,
    sup: &Arc<Supervisor>,
    store: &Store,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    while outputs(http).len() < n && std::time::Instant::now() < deadline {
        sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        outputs(http).len() >= n,
        "outputs={:?}, actual={:?}",
        outputs(http),
        store.actual("test")
    );
}
async fn wait_bootstrap(sup: &Arc<Supervisor>, store: &Store) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if sup
            .checkpoint_inventory("test")
            .await
            .ok()
            .is_some_and(|v| v.storage.current.is_some())
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bootstrap failed: {:?}",
            store.actual("test")
        );
        sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn checkpoint_when_idle(sup: &Arc<Supervisor>) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match sup.checkpoint_named("test").await {
            Ok(id) => return id,
            Err(e)
                if e.code == sparrow_model::ErrorCode::ResourceExhausted
                    && e.message == "checkpoint already queued or active"
                    && std::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("checkpoint failed: {e}"),
        }
    }
}

async fn reader_consumer(
    context: &sparrow_testkit::nats::jetstream::Context,
    base: &str,
) -> sparrow_testkit::nats::jetstream::consumer::PullConsumer {
    let kv = context.get_key_value("OWNERS").await.unwrap();
    let entry = kv.entry(format!("INPUT.{base}")).await.unwrap().unwrap();
    let nonce: String = entry.value[36..]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    context
        .get_stream("INPUT")
        .await
        .unwrap()
        .get_consumer(&format!("{base}_{nonce}"))
        .await
        .unwrap()
}
async fn wait_acked(context: &sparrow_testkit::nats::jetstream::Context, base: &str, cut: u64) {
    let mut consumer = reader_consumer(context, base).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let info = consumer.info().await.unwrap();
            if info.ack_floor.stream_sequence == cut && info.num_ack_pending == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("broker must confirm the complete committed prefix");
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_backlog_bootstrap_template_periodic_and_manual_full_race_preserve_attempt() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        NatsSandbox::publish(&context, 1, 128).await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let mut spec = spec(
            &fixture,
            &http.url(),
            "race",
            "SELECT device_id, v FROM sensors",
        );
        spec.checkpoint.as_mut().unwrap().interval_ms = Some(1000);
        spec.checkpoint.as_mut().unwrap().timeout_ms = 5000;
        spec.source.jetstream.as_mut().unwrap().max_pending = 8;
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        let attempt = store.actual("test").unwrap().attempt_id;
        wait_bootstrap(&sup, &store).await;
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let sup = sup.clone();
            tasks.spawn(async move {
                for _ in 0..32 {
                    match sup.checkpoint_named("test").await {
                        Ok(_) => {}
                        Err(e) => assert_eq!(e.code, sparrow_model::ErrorCode::ResourceExhausted),
                    }
                    tokio::time::sleep(Duration::from_millis(3)).await;
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        wait_outputs(&http, 128, &sup, &store).await;
        checkpoint_when_idle(&sup).await;
        wait_acked(&context, "race", 128).await;
        let (_, control) = sup.checkpoint_snapshot("test").unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status = control.snapshot();
                if status.last_trigger == Some("periodic") && !status.active {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the actual 1s template scheduler must complete a checkpoint");
        assert_eq!(store.actual("test").unwrap().attempt_id, attempt);
        let rows = outputs(&http);
        assert_eq!(rows.len(), 128);
        assert_eq!(
            rows.iter()
                .map(|r| r["data"]["v"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            (1..=128).collect::<Vec<_>>()
        );
        let info = sup.checkpoint_inventory("test").await.unwrap();
        assert!(info.revision > 0);
        let first =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap();
        assert_eq!(
            first
                .recover_pipeline_required()
                .unwrap()
                .source
                .offset_bytes,
            128
        );
        sup.kill_named("test").await.unwrap();
        http.stop().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_slow_successful_http_checkpoint_timeout_keeps_attempt_then_commits() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let mut spec = spec(
            &fixture,
            &http.url(),
            "slow",
            "SELECT device_id, v FROM sensors",
        );
        spec.sink.batch_rows = Some(1);
        spec.sink.linger_ms = Some(0);
        spec.checkpoint.as_mut().unwrap().timeout_ms = 100;
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        let attempt = store.actual("test").unwrap().attempt_id;
        http.set_delay_ms(350);
        NatsSandbox::publish(&context, 1, 6).await;
        wait_outputs(&http, 1, &sup, &store).await;
        assert!(sup.checkpoint_named("test").await.is_err());
        let mut consumer = reader_consumer(&context, "slow").await;
        assert_eq!(consumer.info().await.unwrap().ack_floor.stream_sequence, 0);
        wait_outputs(&http, 6, &sup, &store).await;
        http.set_delay_ms(0);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if sup.checkpoint_named("test").await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        wait_acked(&context, "slow", 6).await;
        sup.converge_once().await.unwrap();
        assert_eq!(store.actual("test").unwrap().attempt_id, attempt);
        assert_eq!(outputs(&http).len(), 6);
        sup.kill_named("test").await.unwrap();
        http.stop().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_failed_current_publication_never_acks_broker_pending_prefix() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let spec = spec(
            &fixture,
            &http.url(),
            "commitfail",
            "SELECT device_id, v FROM sensors",
        );
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        NatsSandbox::publish(&context, 1, 6).await;
        wait_outputs(&http, 6, &sup, &store).await;
        let mut consumer = reader_consumer(&context, "commitfail").await;
        assert_eq!(consumer.info().await.unwrap().num_ack_pending, 6);
        let dir = std::path::Path::new(spec.checkpoint_dir.as_ref().unwrap());
        let before = std::fs::read(dir.join("CURRENT")).unwrap();
        std::fs::create_dir(dir.join("CURRENT.tmp")).unwrap();
        assert!(sup.checkpoint_named("test").await.is_err());
        sup.kill_named("test").await.unwrap();
        let info = consumer.info().await.unwrap();
        assert_eq!(info.num_ack_pending, 6);
        assert_eq!(info.ack_floor.stream_sequence, 0);
        assert_eq!(std::fs::read(dir.join("CURRENT")).unwrap(), before);
        http.stop().await;
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_pipeline_uncommitted_crash_replays_same_output_ids_then_ack_after_checkpoint() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let spec = spec(
            &fixture,
            &http.url(),
            "base",
            "SELECT device_id, v FROM sensors",
        );
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        NatsSandbox::publish(&context, 1, 6).await;
        wait_outputs(&http, 6, &sup, &store).await;
        let initial = outputs(&http);
        let inventory = sup.checkpoint_inventory("test").await.unwrap();
        let cut =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap()
                .recover_pipeline_required()
                .unwrap();
        assert_eq!(
            cut.source.offset_bytes, 0,
            "read/published/HTTP-success must NOT advance CURRENT"
        );
        assert!(inventory.storage.current.is_some());
        sup.kill_named("test").await.unwrap();
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        request_start(&store, "test", "test").unwrap();
        sup.converge_once().await.unwrap();
        wait_outputs(&http, 12, &sup, &store).await;
        let replayed = outputs(&http);
        assert_eq!(
            &replayed[..6],
            &replayed[6..],
            "uncommitted HTTP output must replay with identical IDs and values"
        );
        assert_eq!(
            initial
                .iter()
                .map(|r| r["data"]["v"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            (1..=6).collect::<Vec<_>>()
        );
        sup.checkpoint_named("test").await.unwrap();
        // Wait for the source actor to complete post-commit double-ACKs.
        tokio::time::sleep(Duration::from_millis(100)).await;
        sup.kill_named("test").await.unwrap();
        request_start(&store, "test", "test").unwrap();
        sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            outputs(&http).len(),
            12,
            "committed prefix must not replay after reader rebuild"
        );
        NatsSandbox::publish(&context, 7, 12).await;
        wait_outputs(&http, 18, &sup, &store).await;
        sup.checkpoint_named("test").await.unwrap();
        let all = outputs(&http);
        let unique: std::collections::BTreeSet<_> =
            all.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(unique.len(), 12);
        sup.kill_named("test").await.unwrap();
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        http.stop().await;
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_pipeline_count_window_restores_partial_state_and_no_input_is_acked_on_poison() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let spec = spec(
            &fixture,
            &http.url(),
            "count",
            "SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)",
        );
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), true, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        NatsSandbox::publish(&context, 1, 5).await;
        wait_outputs(&http, 1, &sup, &store).await;
        // The count-window output alone doesn't prove row 5 entered the cut;
        // manual barrier runs only after the source actor publishes prior rows.
        tokio::time::sleep(Duration::from_millis(100)).await;
        sup.checkpoint_named("test").await.unwrap();
        let cut =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap()
                .recover_pipeline_required()
                .unwrap();
        assert_eq!(cut.source.offset_bytes, 5);
        assert_eq!(cut.windows.len(), 1);
        assert_eq!(cut.windows[0].entries[0].count, 2);
        sup.kill_named("test").await.unwrap();
        request_start(&store, "test", "test").unwrap();
        sup.converge_once().await.unwrap();
        NatsSandbox::publish(&context, 6, 9).await;
        wait_outputs(&http, 3, &sup, &store).await;
        assert_eq!(
            outputs(&http)
                .iter()
                .map(|r| r["data"]["s"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![6, 15, 24]
        );
        sup.checkpoint_named("test").await.unwrap();
        context
            .publish("input.rows", "not-json".into())
            .await
            .unwrap()
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            sup.converge_once().await.unwrap();
            let actual = store.actual("test").unwrap();
            if actual.restart_blocked {
                assert!(actual.last_error.unwrap().contains("source_sequence"));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "poison did not fail/hold"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let cut =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap()
                .recover_pipeline_required()
                .unwrap();
        assert_eq!(cut.source.offset_bytes, 9);
        assert_eq!(outputs(&http).len(), 3);
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        http.stop().await;
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_pipeline_failed_http_and_conflicting_owner_never_advance_checkpoint() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let spec = spec(
            &fixture,
            &http.url(),
            "blocked",
            "SELECT device_id, v FROM sensors",
        );
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), true, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        let mut other = spec.clone();
        other.checkpoint_dir = Some(
            fixture
                .root
                .join("other-checkpoint")
                .to_string_lossy()
                .into_owned(),
        );
        store.put_pipeline("other", &other, None).unwrap();
        request_start(&store, "other", "test").unwrap();
        sup.converge_once().await.unwrap();
        let refused = store.actual("other").unwrap();
        assert_ne!(refused.status, "running");
        assert!(refused
            .last_error
            .unwrap()
            .contains("another checkpoint owner"));
        http.set_status(400);
        NatsSandbox::publish(&context, 1, 8).await;
        wait_outputs(&http, 1, &sup, &store).await;
        // A failed required sink must fail promptly without waiting for the
        // 10s periodic checkpoint or another input to fill the pending budget.
        for _ in 0..200 {
            sup.converge_once().await.unwrap();
            if store.actual("test").unwrap().restart_blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(store.actual("test").unwrap().restart_blocked);
        assert!(sup.checkpoint_named("test").await.is_err());
        let failed = outputs(&http);
        assert!(!failed.is_empty() && failed.len() <= 8);
        let checkpoint =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap()
                .recover_pipeline_required()
                .unwrap();
        assert_eq!(checkpoint.source.offset_bytes, 0);
        assert_eq!(checkpoint.next_output.unwrap().first(), 1);
        http.set_status(200);
        request_start(&store, "test", "test").unwrap();
        sup.converge_once().await.unwrap();
        wait_outputs(&http, failed.len() + 8, &sup, &store).await;
        let all = outputs(&http);
        assert_eq!(failed.as_slice(), &all[failed.len()..failed.len() * 2]);
        assert_eq!(
            all[failed.len()..]
                .iter()
                .map(|r| r["data"]["v"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            (1..=8).collect::<Vec<_>>()
        );
        sup.checkpoint_named("test").await.unwrap();
        sup.kill_named("test").await.unwrap();
        sup.kill_named("other").await.unwrap();
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        http.stop().await;
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_pipeline_bounded_full_input_checkpoint_and_broker_restart() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let mut fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let mut spec = spec(
            &fixture,
            &http.url(),
            "full",
            "SELECT device_id, v FROM sensors",
        );
        spec.source.jetstream.as_mut().unwrap().max_pending = 8;
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        NatsSandbox::publish(&context, 1, 128).await;
        wait_outputs(&http, 128, &sup, &store).await;
        checkpoint_when_idle(&sup).await;
        let checkpoint =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap()
                .recover_pipeline_required()
                .unwrap();
        assert_eq!(checkpoint.source.offset_bytes, 128);
        assert!(
            checkpoint.checkpoint_id >= 16,
            "full input must checkpoint instead of deadlocking or dropping"
        );
        let initial = outputs(&http);
        assert_eq!(initial.len(), 128);
        assert_eq!(
            initial
                .iter()
                .map(|r| r["data"]["v"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            (1..=128).collect::<Vec<_>>()
        );
        // Real broker process kill/restart, not a mocked reconnection callback.
        drop(context);
        fixture.restart().await;
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        loop {
            sup.converge_once().await.unwrap();
            if store.actual("test").unwrap().status == "running"
                && sup
                    .flow_snapshot("test")
                    .unwrap()
                    .is_some_and(|s| !s.cancel_requested)
            {
                // Allow stale pre-restart attempt to observe socket closure first.
                tokio::time::sleep(Duration::from_millis(100)).await;
                sup.converge_once().await.unwrap();
                if store.actual("test").unwrap().status == "running"
                    && !sup.flow_snapshot("test").unwrap().unwrap().cancel_requested
                {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "broker restart not recovered: {:?}",
                store.actual("test")
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let context = fixture.context().await;
        NatsSandbox::publish(&context, 129, 144).await;
        wait_outputs(&http, 144, &sup, &store).await;
        assert_eq!(outputs(&http).len(), 144);
        assert_eq!(&outputs(&http)[..128], initial.as_slice());
        sup.kill_named("test").await.unwrap();
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        http.stop().await;
    });
}

#[test]
#[ignore = "requires isolated pinned SPARROW_NATS_SERVER"]
fn k2_runtime_broker_redelivery_does_not_update_count_window_twice() {
    use sparrow_testkit::nats::jetstream;
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let fixture = NatsSandbox::start().await;
        let context = fixture.provision().await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = catalog(&fixture, &http);
        let spec = spec(
            &fixture,
            &http.url(),
            "duplicates",
            "SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)",
        );
        store.put_pipeline("test", &spec, None).unwrap();
        request_start(&store, "test", "test").unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), true, None).unwrap();
        sup.converge_once().await.unwrap();
        wait_bootstrap(&sup, &store).await;
        let kv = context.get_key_value("OWNERS").await.unwrap();
        let binding = kv.get("INPUT.duplicates").await.unwrap().unwrap();
        let name = format!(
            "duplicates_{}",
            binding[36..]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let stream = context.get_stream("INPUT").await.unwrap();
        let mut consumer: jetstream::consumer::PullConsumer =
            stream.get_consumer(&name).await.unwrap();
        let mut config = consumer.cached_info().config.clone();
        config.ack_wait = Duration::from_millis(100);
        // Deliberate broker fault injection: redeliver faster than the actor's
        // 5s progress interval, without changing stream/cut/reader identity.
        stream.update_consumer(config).await.unwrap();
        NatsSandbox::publish(&context, 1, 6).await;
        wait_outputs(&http, 2, &sup, &store).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            consumer.info().await.unwrap().delivered.consumer_sequence > 6,
            "broker must actually redeliver"
        );
        assert_eq!(
            outputs(&http)
                .iter()
                .map(|r| r["data"]["s"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![6, 15]
        );
        assert_eq!(store.actual("test").unwrap().status, "running");
        checkpoint_when_idle(&sup).await;
        let cut =
            sparrow_runtime::CheckpointStore::open_readonly(spec.checkpoint_dir.as_ref().unwrap())
                .unwrap()
                .recover_pipeline_required()
                .unwrap();
        assert_eq!(cut.source.offset_bytes, 6);
        assert_eq!(cut.ingested_rows, 6);
        assert_eq!(cut.next_output.unwrap().first(), 3);
        sup.kill_named("test").await.unwrap();
        assert_eq!(kernel.process_owner().usage().physical_bytes, 0);
        http.stop().await;
    });
}

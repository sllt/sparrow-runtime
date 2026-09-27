//! Whole live profile tests: real MQTT packets -> connector FIFO -> Kernel ->
//! HTTP. The fault broker is deliberately controlled, not a production broker.
use super::{outputs, parse, scratch, store, wait_output};
use crate::{request_start, PipelineSpec, Supervisor};
use serde_json::{json, Value};
use sparrow_connectors::mqtt::{
    codec::{Packet, Publish},
    io::{write_packet, MqttFramedReader, MqttStream},
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

fn configuration(port: u16, url: &str) -> Value {
    let mut value: Value = serde_json::from_slice(include_bytes!(
        "../../../deploy/pipeline-iot-mqtt-silence.json"
    ))
    .unwrap();
    value["stream"] = json!("sensors");
    value["graph"]["nodes"][0]["table"] = json!("sensors");
    value["source"]["port"] = json!(port);
    value["sink"]["url"] = json!(url);
    value["graph"]["nodes"][1]["iot"]["timing"] = json!({"kind":"silence","clock":"live",
        "duration_micros":800000,"max_observation_gap_micros":400000,"registered_keys":[["registered"]]});
    value
}

#[test]
fn live_silence_admission_template_explain_and_restore_rejections() {
    let store = store();
    let value = configuration(1883, "http://127.0.0.1:1/events");
    let spec = parse(&value);
    let plan = crate::bind_plan_with_store(&store, &spec, "time", 1).unwrap();
    crate::validate_aligned_plan(&spec, &plan).unwrap();
    assert!(sparrow_plan::CheckpointPlan::from_physical(&plan).is_err());
    let explain = crate::effective_guarantees_with_plan(&spec, &plan);
    assert_eq!(explain["aligned_eligible"], false);
    assert_eq!(explain["silence"]["live_eligible"], true);
    assert!(explain["silence"]["snapshot_version"].is_null());
    for variant in 0..13 {
        let mut bad = value.clone();
        match variant {
            0 => bad["recovery"] = json!("aligned"),
            1 => bad["checkpoint_dir"] = json!("/tmp/live-not-a-checkpoint"),
            2 => bad["checkpoint"] = json!({"interval_ms":100}),
            3 => bad["restore"] = json!({"kind":"checkpoint","snapshot_id":"aligned"}),
            4 => bad["source"]["kind"] = json!("file"),
            5 => bad["source"]["clean_session"] = json!(false),
            6 => bad["source"]["qos"] = json!(1),
            7 => bad["sink"]["kind"] = json!("log"),
            8 => bad["sink"]["skip_verify"] = json!(true),
            9 => bad["graph"]["nodes"][1]["iot"]["timing"]["clock"] = json!("paused"),
            10 => {
                bad["graph"]["nodes"][1]["iot"]["timing"]["max_observation_gap_micros"] =
                    json!(99999)
            }
            11 => {
                bad["graph"]["nodes"][1]["iot"]["timing"]["max_observation_gap_micros"] =
                    json!(120000001)
            }
            _ => bad["graph"]["nodes"][1]["iot"]["timing"]["kind"] = json!("hold_for"),
        }
        let rejected =
            PipelineSpec::from_json(&serde_json::to_vec(&bad).unwrap()).and_then(|spec| {
                let plan = crate::bind_plan_with_store(&store, &spec, "time", 1)?;
                crate::validate_aligned_plan(&spec, &plan)
            });
        assert!(rejected.is_err(), "variant {variant}");
    }
    let kernel = crate::host_kernel().unwrap();
    let (tx, rx) = sparrow_io::observed::channel(2);
    let request = sparrow_runtime::JobRequest::new(
        plan.clone(),
        vec![],
        sparrow_runtime::SharedCapture::disabled(),
    )
    .with_live_events(rx)
    .with_live_silence([1; 16])
    .unwrap();
    let job = kernel.submit(request).unwrap();
    kernel.block_on(job.stop()).unwrap();
    drop(tx);
    let request =
        sparrow_runtime::JobRequest::new(plan, vec![], sparrow_runtime::SharedCapture::disabled());
    assert!(
        kernel.submit(request).is_err(),
        "embedded callers need explicit live ingress admission"
    );
}

#[derive(Clone)]
enum Command {
    Publish(Vec<u8>, bool),
    Responsive(bool),
    Disconnect,
}
struct Broker {
    port: u16,
    command: mpsc::Sender<(Command, oneshot::Sender<()>)>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Broker {
    async fn start(responsive: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (command, mut commands) = mpsc::channel::<(Command, oneshot::Sender<()>)>(8);
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            let mut responsive = responsive;
            loop {
                let socket = tokio::select! { biased; _ = stop.cancelled() => return, socket = listener.accept() => socket.unwrap().0 };
                socket.set_nodelay(true).unwrap();
                let mut stream: MqttStream = Box::pin(socket);
                let mut reader = MqttFramedReader::new();
                let connection = async {
                    assert!(matches!(
                        reader.next(&mut stream).await?,
                        Packet::Connect(_)
                    ));
                    write_packet(
                        &mut stream,
                        &Packet::ConnAck {
                            session_present: false,
                            return_code: 0,
                        },
                    )
                    .await?;
                    assert!(matches!(
                        reader.next(&mut stream).await?,
                        Packet::Subscribe { .. }
                    ));
                    write_packet(
                        &mut stream,
                        &Packet::SubAck {
                            packet_id: 1,
                            codes: vec![0],
                        },
                    )
                    .await?;
                    loop {
                        tokio::select! { biased;
                            _ = stop.cancelled() => return Ok::<(), sparrow_connectors::ConnectorError>(()),
                            command = commands.recv() => {
                                let Some((command, ack)) = command else { return Ok(()) };
                                match command {
                                    Command::Responsive(value) => responsive = value,
                                    Command::Disconnect => { let _ = ack.send(()); return Ok(()); }
                                    Command::Publish(payload, retain) => {
                                        write_packet(&mut stream, &Packet::Publish(Publish {
                                            dup:false, qos:0, retain, topic:"sensors/json".into(), packet_id:None, payload,
                                        })).await?;
                                    }
                                }
                                let _ = ack.send(());
                            }
                            packet = reader.next(&mut stream) => match packet? {
                                Packet::PingReq if responsive => write_packet(&mut stream, &Packet::PingResp).await?,
                                Packet::Disconnect => return Ok(()),
                                _ => {},
                            }
                        }
                    }
                };
                tokio::select! { biased; _ = stop.cancelled() => return, _ = connection => {} }
            }
        });
        Self {
            port,
            command,
            cancel,
            task,
        }
    }
    async fn command(&self, command: Command) {
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let (tx, rx) = oneshot::channel();
                self.command.send((command.clone(), tx)).await.unwrap();
                if rx.await.is_ok() {
                    break;
                }
                // A deliberate ping timeout can race a socket write. A repeat
                // is permitted only for this live-best-effort traffic fixture.
            }
        })
        .await
        .unwrap();
    }
    async fn row(&self, key: &str, retained: bool) {
        self.command(Command::Publish(
            serde_json::to_vec(&json!({"device_id":key,"active":true})).unwrap(),
            retained,
        ))
        .await;
    }
    async fn stop(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}

async fn wait_io(
    sup: &Arc<Supervisor>,
    predicate: impl Fn(&sparrow_connectors::IoSnapshot) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            sup.converge_once().await.unwrap();
            if predicate(&sup.io_snapshot().await) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
fn data(row: &Value) -> &Value {
    row.get("data").unwrap_or(row)
}

#[test]
fn live_silence_mqtt_http_lifecycle_retained_and_restart_generation() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    let dir = scratch();
    kernel.block_on(async {
        let broker = Broker::start(true).await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", broker.port).unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let spec = parse(&configuration(broker.port, &http.url()));
        store.put_pipeline("time", &spec, None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        wait_io(&sup, |io| io.mqtt_feed_probes >= 2).await;
        broker.row("retained-only", true).await;
        broker.row("seen", false).await;
        wait_output(&sup, &store, &http, 2).await;
        let rows = outputs(&http);
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| data(row)["sparrow_silence_event"] == "silent"));
        assert!(rows
            .iter()
            .all(|row| data(row)["device_id"] != "retained-only"));
        let first_generation = data(&rows[0])["sparrow_silence_generation"].clone();
        broker.row("seen", true).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(outputs(&http).len(), 2, "retained data cannot resume");
        broker.row("seen", false).await;
        wait_output(&sup, &store, &http, 3).await;
        let rows = outputs(&http);
        assert_eq!(data(&rows[2])["sparrow_silence_event"], "resumed");
        assert_eq!(data(&rows[2])["sparrow_silence_episode"], 1);
        broker.command(Command::Disconnect).await;
        wait_io(&sup, |io| {
            io.mqtt_reconnects > 0 && io.mqtt_feed_probes >= 5
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            outputs(&http).len(),
            3,
            "reconnect itself cannot resume or bypass new grace"
        );
        wait_output(&sup, &store, &http, 4).await;
        assert_eq!(data(&outputs(&http)[3])["sparrow_silence_episode"], 2);
        sup.kill_named("time").await.unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            outputs(&http).len(),
            4,
            "pipeline restart requires a full new grace"
        );
        wait_output(&sup, &store, &http, 5).await;
        let rows = outputs(&http);
        assert_eq!(data(&rows[4])["device_id"], "registered");
        assert_ne!(
            data(&rows[4])["sparrow_silence_generation"],
            first_generation
        );
        assert!(data(&rows[4])["sparrow_silence_last_seen"].is_null());
        assert_eq!(data(&rows[4])["sparrow_silence_never_seen"], true);
        assert!(!dir.0.join("CURRENT").exists());
        sup.stop_all().await;
        broker.stop().await;
        http.stop().await;
        assert_eq!(kernel.live_tasks(), 0);
    });
}

#[test]
fn live_silence_no_pingresp_even_with_traffic_never_authorizes_silent() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let broker = Broker::start(false).await;
        let http = sparrow_connectors::HttpCapture::start().await.unwrap();
        let store = store();
        store.put_allow("127.0.0.1", broker.port).unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        store
            .put_pipeline(
                "time",
                &parse(&configuration(broker.port, &http.url())),
                None,
            )
            .unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        for _ in 0..16 {
            broker.row("active", false).await;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        let io = sup.io_snapshot().await;
        assert!(io.mqtt_decoded > 0 && io.mqtt_ping_timeouts > 0 && io.mqtt_reconnects > 0);
        assert_eq!(io.mqtt_feed_probes, 0);
        assert!(outputs(&http).is_empty());
        broker.command(Command::Responsive(true)).await;
        wait_io(&sup, |io| io.mqtt_feed_probes > 0).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            outputs(&http).is_empty(),
            "no accumulated grace from unresponsive sessions"
        );
        wait_output(&sup, &store, &http, 2).await;
        sup.stop_all().await;
        broker.stop().await;
        http.stop().await;
    });
}

#[test]
fn live_silence_decode_loss_requires_full_grace_and_fail_policy_stops() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        for fail in [false, true] {
            let broker = Broker::start(true).await;
            let http = sparrow_connectors::HttpCapture::start().await.unwrap();
            let store = store();
            store.put_allow("127.0.0.1", broker.port).unwrap();
            store.put_allow("127.0.0.1", http.port()).unwrap();
            let mut value = configuration(broker.port, &http.url());
            value["fail_on_decode"] = json!(fail);
            value["graph"]["nodes"][1]["iot"]["timing"]["duration_micros"] = json!(1600000);
            store.put_pipeline("time", &parse(&value), None).unwrap();
            let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
            request_start(&store, "time", "test").unwrap();
            wait_io(&sup, |io| io.mqtt_feed_probes >= 2).await;
            tokio::time::sleep(Duration::from_millis(850)).await;
            broker
                .command(Command::Publish(b"malformed-json".to_vec(), false))
                .await;
            // Do not converge a failed attempt into an automatic fresh retry:
            // inspect this attempt's outcome before any supervisor replacement.
            tokio::time::sleep(Duration::from_millis(850)).await;
            assert!(
                outputs(&http).is_empty(),
                "decode loss cannot preserve the preceding grace"
            );
            if fail {
                assert_eq!(
                    kernel.live_tasks(),
                    0,
                    "fail_on_decode cancels the pipeline"
                );
            } else {
                wait_output(&sup, &store, &http, 1).await;
                assert_eq!(data(&outputs(&http)[0])["device_id"], "registered");
                assert_eq!(sup.io_snapshot().await.mqtt_dropped_bad, 1);
            }
            sup.stop_all().await;
            broker.stop().await;
            http.stop().await;
        }
    });
}

#[test]
fn live_silence_stalled_http_full_ingress_remains_cancellable_and_bounded() {
    let kernel = Arc::new(crate::host_kernel().unwrap());
    kernel.block_on(async {
        let broker = Broker::start(true).await;
        let http = super::HeldHttp::start().await;
        let store = store();
        store.put_allow("127.0.0.1", broker.port).unwrap();
        store.put_allow("127.0.0.1", http.port()).unwrap();
        let mut value = configuration(broker.port, &http.url());
        value["source"]["inbox_capacity"] = json!(2);
        value["source"]["inbox_wait_ms"] = json!(0);
        value["sink"]["outbox_capacity"] = json!(1);
        value["sink"]["max_inflight"] = json!(1);
        value["graph"]["nodes"][1]["iot"]["timing"]["registered_keys"] = json!((0..128)
            .map(|i| vec![format!("key-{i}")])
            .collect::<Vec<_>>());
        store.put_pipeline("time", &parse(&value), None).unwrap();
        let sup = Supervisor::new(store.clone(), kernel.clone(), false, None).unwrap();
        request_start(&store, "time", "test").unwrap();
        sup.converge_once().await.unwrap();
        http.wait(1).await;
        for _ in 0..64 {
            broker.row("key-0", false).await;
        }
        wait_io(&sup, |io| io.mqtt_dropped_full > 0).await;
        let io = sup.io_snapshot().await;
        // QueueOccupancy includes the ONE popped event waiting on the first
        // mailbox; the observed FIFO itself still has exactly two slots.
        assert!(
            io.mqtt_feed_breaks > 1 && io.mqtt_inbox_peak_items <= 3,
            "{io:?}"
        );
        tokio::time::timeout(Duration::from_secs(2), sup.stop_all())
            .await
            .unwrap();
        assert_eq!(kernel.live_tasks(), 0);
        assert_eq!(kernel.metrics.snapshot().iot_state_keys, 0);
        assert_eq!(kernel.process_owner().accounting_errors_total(), 0);
        http.release();
        broker.stop().await;
        http.stop().await;
    });
}

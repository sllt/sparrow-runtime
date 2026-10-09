//! Explicit opt-in broker tests. Only children and private directories created
//! by this fixture are stopped; no installed daemon/production stream touched.
use super::*;
use async_nats::jetstream::{
    consumer::{pull, AckPolicy, DeliverPolicy},
    stream,
};
use futures_util::StreamExt;
use sparrow_model::{MemoryOwner, ResourceBudget};
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    time::Duration,
};

#[path = "feed_conformance.rs"] mod feed_conformance;

static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Broker {
    process: Option<Child>,
    root: PathBuf,
    port: u16,
}
impl Broker {
    async fn start() -> Self {
        let binary = std::env::var_os("SPARROW_NATS_SERVER")
            .expect("set SPARROW_NATS_SERVER to the pinned isolated test binary");
        let parent = std::env::var_os("SPARROW_TEST_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = parent.join(format!(
            "k2-nats-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&root).unwrap();
        let bind = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = bind.local_addr().unwrap().port();
        drop(bind);
        let config=format!("host: 127.0.0.1\nport: {port}\nmax_payload: 65536\njetstream {{\n store_dir: {}\n max_file_store: 256MB\n max_memory_store: 16MB\n sync_interval: always\n}}\n",serde_json::to_string(&root.join("data")).unwrap());
        std::fs::write(root.join("nats.conf"), config).unwrap();
        let mut b = Self {
            process: None,
            root,
            port,
        };
        b.launch(binary).await;
        b
    }
    async fn launch(&mut self, binary: std::ffi::OsString) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("nats.log"))
            .unwrap();
        self.process = Some(
            Command::new(binary)
                .arg("-c")
                .arg(self.root.join("nats.conf"))
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    self.process.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "isolated NATS server exited; inspect nats.log"
                );
                if tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn restart(&mut self) {
        self.stop();
        self.launch(std::env::var_os("SPARROW_NATS_SERVER").unwrap())
            .await;
    }
    fn stop(&mut self) {
        if let Some(mut c) = self.process.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
    async fn connect(&self, owner: &Arc<MemoryOwner>) -> Connection {
        Connection::open(
            &ConnectionConfig {
                servers: vec![format!("nats://127.0.0.1:{}", self.port)],
                token_secret: None,
                subscription_capacity: 8,
                pull_bytes: 72 * 1024,
            },
            &crate::TargetPolicy::allow("127.0.0.1", self.port),
            &crate::MapSecretResolver::empty(),
            owner,
        )
        .await
        .unwrap()
    }
}
impl Drop for Broker {
    fn drop(&mut self) {
        self.stop();
    }
}
fn stream_config(name: &str) -> stream::Config {
    stream::Config {
        name: name.into(),
        subjects: vec![format!("{name}.>")],
        storage: stream::StorageType::File,
        retention: stream::RetentionPolicy::Limits,
        max_bytes: 16 * 1024 * 1024,
        max_message_size: 65536,
        max_consumers: 16,
        num_replicas: 1,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; bounded ACK concurrency and retries"]
async fn k2_ack_worker_confirms_concurrently_retries_negative_replies_and_joins() {
    use super::ack::{AckDriver, ACK_CONCURRENCY};
    use super::reader::AckToken;
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let responder = async_nats::ConnectOptions::new()
        .subscription_capacity(32)
        .connect(format!("nats://127.0.0.1:{}", broker.port))
        .await
        .unwrap();
    let mut subscription = responder.subscribe("fixture.confirm").await.unwrap();
    // SDK flush only drains local writes. This same-connection round trip
    // proves SUB reached the broker before another connection sends requests.
    responder.request("$JS.API.INFO", "".into()).await.unwrap();
    let mut driver = AckDriver::start(connection.client.clone(), &owner).unwrap();
    let before = owner.usage().reservation_bytes;
    for sequence in 1..=ACK_CONCURRENCY as u64 {
        driver
            .enqueue(sequence, AckToken("fixture.confirm".into()))
            .unwrap();
    }
    // Do not reply until ALL requests arrive: a serial confirmer times out.
    let mut first = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..ACK_CONCURRENCY {
            first.push(subscription.next().await.unwrap());
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "confirmations must be concurrently in flight; received={}",
            first.len()
        )
    });
    for message in first {
        connection
            .client
            .publish(message.reply.unwrap(), "negative".into())
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..ACK_CONCURRENCY {
            let message = subscription.next().await.unwrap();
            connection
                .client
                .publish(message.reply.unwrap(), "".into())
                .await
                .unwrap();
        }
        while driver.active > 0 {
            driver.wake.notified().await;
            while let Some((_, result)) = driver.completed().unwrap() {
                result.unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        driver.retries.load(std::sync::atomic::Ordering::Relaxed),
        ACK_CONCURRENCY as u64
    );
    assert_eq!(owner.usage().reservation_bytes, before);
    driver
        .enqueue(100, AckToken("fixture.confirm".into()))
        .unwrap();
    let _unanswered = subscription.next().await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), driver.close())
        .await
        .unwrap()
        .unwrap();
    drop(subscription);
    connection.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; ownership policy negatives"]
async fn k2_reader_refuses_expiring_or_evicting_ownership_and_runtime_policy_changes() {
    let broker = Broker::start().await;
    let admin = async_nats::connect(format!("nats://127.0.0.1:{}", broker.port))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(admin);
    js.create_stream(stream_config("INPUT")).await.unwrap();
    for case in 0..4 {
        let bucket = format!("OWNERS_{case}");
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: bucket.clone(),
            history: if case == 0 { 2 } else { 1 },
            max_age: if case == 1 {
                Duration::from_secs(3600)
            } else {
                Duration::ZERO
            },
            max_bytes: 1024 * 1024,
            storage: stream::StorageType::File,
            num_replicas: 1,
            ..Default::default()
        })
        .await
        .unwrap();
        if case == 2 {
            let mut kv = js.get_stream(format!("KV_{bucket}")).await.unwrap();
            let mut config = kv.info().await.unwrap().config.clone();
            config.discard = stream::DiscardPolicy::Old;
            js.update_stream(config).await.unwrap();
        }
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let connection = broker.connect(&owner).await;
        let opened = Reader::open(
            connection,
            ReaderConfig {
                namespace: "test".into(),
                stream: "INPUT".into(),
                consumer: format!("reader_{case}"),
                ownership_bucket: bucket.clone(),
                max_pending: 8,
                pending_bytes: 262144,
                pull_messages: 4,
                pull_bytes: 73728,
                payload_format: Default::default(),
            },
            owner.clone(),
            [7; 32],
            [case + 1; 16],
            None,
        )
        .await;
        if case < 3 {
            let error = opened.err().expect("invalid bucket accepted");
            assert_eq!(error.code, sparrow_model::ErrorCode::UnsupportedRestore);
            assert!(error.message.contains("ownership bucket"));
        } else {
            let mut reader = opened.unwrap();
            let mut kv = js.get_stream(format!("KV_{bucket}")).await.unwrap();
            let mut config = kv.info().await.unwrap().config.clone();
            config.max_age = Duration::from_secs(3600);
            js.update_stream(config).await.unwrap();
            assert_eq!(
                reader.verify().await.unwrap_err().code,
                sparrow_model::ErrorCode::UnsupportedRestore
            );
            reader.close().await.unwrap();
        }
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}
fn consumer_config(name: &str, start: u64) -> pull::Config {
    pull::Config {
        name: Some(name.into()),
        durable_name: Some(name.into()),
        deliver_policy: DeliverPolicy::ByStartSequence {
            start_sequence: start,
        },
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_millis(150),
        max_ack_pending: 4,
        max_deliver: -1,
        max_waiting: 1,
        max_batch: 4,
        max_bytes: 65536,
        max_expires: Duration::from_secs(1),
        ..Default::default()
    }
}
async fn batch(
    consumer: &async_nats::jetstream::consumer::PullConsumer,
    count: usize,
) -> Vec<async_nats::jetstream::Message> {
    let mut stream = consumer
        .batch()
        .max_messages(count)
        .max_bytes(65536)
        .expires(Duration::from_millis(250))
        .messages()
        .await
        .unwrap();
    let mut messages = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(m) = stream.next().await {
            messages.push(m.unwrap());
        }
    })
    .await
    .unwrap();
    assert!(messages.len() <= count);
    messages
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; creates only an isolated broker child"]
async fn k2_nats_sdk_lifetime_is_joined_not_drain_returned() {
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    for _ in 0..20 {
        let connection = broker.connect(&owner).await;
        assert_eq!(
            owner.usage().reservation_bytes,
            512 * 1024 + 32 * 72 * 1024 + 8 * 4608
        );
        connection.check_health().unwrap();
        connection.close().await.unwrap();
        assert_eq!(
            owner.usage().physical_bytes,
            0,
            "close must join event-loop lease, not just enqueue drain"
        );
    }
    // A live SDK context isn't a join handle: drain terminates even with a
    // client clone. It must not keep the connection lease forever either.
    let connection = broker.connect(&owner).await;
    let extra = connection.client.clone();
    connection.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
    drop(extra);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; creates only an isolated broker child"]
async fn k2_nats_pull_progress_redelivery_ack_and_reader_isolation() {
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    let mut stream = js.create_stream(stream_config("INPUT")).await.unwrap();
    for n in 1..=6 {
        js.publish(format!("INPUT.{}", n % 2), format!("{{\"v\":{n}}}").into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    check_stream(stream.info().await.unwrap(), 1).unwrap();
    let consumer = stream
        .create_consumer_strict(consumer_config("reader_a", 1))
        .await
        .unwrap();
    // Exactly one reader name is created; strict create doesn't adopt another
    // reader with a different cut. Local ownership binding is a separate gate.
    assert!(stream
        .create_consumer_strict(consumer_config("reader_a", 2))
        .await
        .is_err());
    let initial = batch(&consumer, 4).await;
    assert_eq!(initial.len(), 4);
    assert_eq!(
        initial
            .iter()
            .map(|m| m.info().unwrap().stream_sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    for _ in 0..4 {
        for m in &initial {
            m.ack_with(async_nats::jetstream::AckKind::Progress)
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let mut consumer = consumer;
    assert_eq!(consumer.info().await.unwrap().num_ack_pending, 4);
    // No real ACK so the prefix must redeliver, not advance to rows 5/6.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let redelivered = batch(&consumer, 4).await;
    assert_eq!(redelivered.len(), 4);
    for (index, m) in redelivered.iter().enumerate() {
        let info = m.info().unwrap();
        assert_eq!(info.stream_sequence, index as u64 + 1);
        assert!(info.delivered >= 2);
        assert_eq!(info.consumer_sequence, index as u64 + 5);
        m.double_ack().await.unwrap();
    }
    assert_eq!(consumer.info().await.unwrap().num_ack_pending, 0);
    let suffix = batch(&consumer, 2).await;
    assert_eq!(suffix.len(), 2);
    assert_eq!(suffix[0].info().unwrap().stream_sequence, 5);
    let old_ack = suffix.into_iter().next().unwrap().split().1;
    drop(initial);
    drop(redelivered);
    drop(consumer);
    stream.delete_consumer("reader_a").await.unwrap();
    let mut replacement = stream
        .create_consumer_strict(consumer_config("reader_b", 5))
        .await
        .unwrap();
    let replay = batch(&replacement, 2).await;
    assert_eq!(replay.len(), 2);
    // Old attempt's reply subject cannot ACK replacement consumer's rows.
    let _ = old_ack.ack().await;
    connection.client.flush().await.unwrap();
    assert_eq!(replacement.info().await.unwrap().num_ack_pending, 2);
    for m in &replay {
        m.double_ack().await.unwrap();
    }
    assert_eq!(replacement.info().await.unwrap().num_ack_pending, 0);
    drop(replay);
    drop(old_ack);
    drop(replacement);
    drop(stream);
    drop(js);
    connection.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; creates only an isolated broker child"]
async fn k2_nats_restart_identity_retention_and_stream_policy() {
    let mut broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    let mut config = stream_config("RETAIN");
    config.max_messages = 4;
    let mut stream = js.create_stream(config).await.unwrap();
    let identity = StreamIdentity::from_info("test_account", stream.info().await.unwrap()).unwrap();
    for n in 1..=8 {
        js.publish("RETAIN.all", format!("{{\"v\":{n}}}").into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let info = stream.info().await.unwrap();
    assert_eq!(info.state.first_sequence, 5);
    assert!(check_stream(info, 1).is_err());
    check_stream(info, 5).unwrap();
    assert!(check_stream(info, 10).is_err());
    assert!(stream.delete_message(5).await.is_err());
    assert!(stream.purge().await.is_err());
    drop(stream);
    drop(js);
    connection.close().await.unwrap();
    broker.restart().await;
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    let mut stream = js.get_stream("RETAIN").await.unwrap();
    assert_eq!(
        identity,
        StreamIdentity::from_info("test_account", stream.info().await.unwrap()).unwrap()
    );
    let mut bad = stream.cached_info().config.clone();
    bad.max_messages_per_subject = 1;
    let updated = js.update_stream(&bad).await.unwrap();
    assert!(check_stream(&updated, 5).is_err());
    drop(updated);
    drop(stream);
    js.delete_stream("RETAIN").await.unwrap();
    let mut new = js.create_stream(stream_config("RETAIN")).await.unwrap();
    assert_ne!(
        identity,
        StreamIdentity::from_info("test_account", new.info().await.unwrap()).unwrap()
    );
    drop(new);
    drop(js);
    connection.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn k2_nats_endpoint_validation_is_closed_and_secret_safe() {
    let policy = crate::TargetPolicy::allow("127.0.0.1", 4222);
    for server in [
        "nats://user:password@127.0.0.1:4222",
        "ws://127.0.0.1:4222",
        "nats://127.0.0.1:4223",
        "nats://127.0.0.1:4222/subject",
        "nats://127.0.0.1:4222?token=secret",
    ] {
        let config = ConnectionConfig {
            servers: vec![server.into()],
            token_secret: None,
            subscription_capacity: 8,
            pull_bytes: 65536,
        };
        let error = config.validate(&policy).unwrap_err();
        assert!(!error.to_string().contains("password"));
        assert!(!error.to_string().contains("token=secret"));
    }
    let config = ConnectionConfig {
        servers: vec!["nats://127.0.0.1:4222".into()],
        token_secret: Some("token".into()),
        subscription_capacity: 8,
        pull_bytes: 65536,
    };
    assert!(config.validate(&policy).is_err());
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; long buffered-fetch/cancellation test"]
async fn k2_reader_buffer_survives_control_cancellation_and_long_mailbox_stall() {
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    js.create_stream(stream_config("INPUT")).await.unwrap();
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: "OWNERS".into(),
        history: 1,
        max_bytes: 1024 * 1024,
        max_value_size: 1024,
        storage: stream::StorageType::File,
        num_replicas: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    for n in 1..=4 {
        js.publish("INPUT.rows", format!("{{\"v\":{n}}}").into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let config = ReaderConfig {
        namespace: "test_account".into(),
        stream: "INPUT".into(),
        consumer: "base".into(),
        ownership_bucket: "OWNERS".into(),
        max_pending: 8,
        pending_bytes: 256 * 1024,
        pull_messages: 4,
        pull_bytes: 72 * 1024,
        payload_format: Default::default(),
    };
    let mut reader = Reader::open(
        connection,
        config.clone(),
        owner.clone(),
        [7; 32],
        [1; 16],
        None,
    )
    .await
    .unwrap();
    assert!(reader.prepare_pull().await.unwrap());
    let ReaderPoll::Record(first) = reader.next().await.unwrap() else {
        panic!("expected first row")
    };
    assert_eq!(first.sequence(), 1);
    reader.published(1).unwrap();
    drop(first);
    // Exercise actual drop of a polled outer select without dropping the
    // reader-owned batch or restarting a pull with a new subscriber.
    {
        let next = reader.next();
        tokio::pin!(next);
        tokio::select! {biased;_ = std::future::ready(())=>{},_=&mut next=>panic!("control must win")}
    }
    // More than SDK Batch's usual expires+5s watchdog, and more than our
    // request deadline. Already-buffered data must still be consumed first.
    tokio::time::sleep(Duration::from_millis(6100)).await;
    let mut saved = None;
    for n in 2..=4 {
        let ReaderPoll::Record(record) = reader.next().await.unwrap() else {
            panic!("buffered row {n} lost")
        };
        assert_eq!(record.sequence(), n);
        reader.published(n).unwrap();
        if n == 3 {
            saved = Some(reader.position(3));
        }
    }
    assert!(matches!(reader.next().await.unwrap(), ReaderPoll::BatchEnd));
    assert_eq!(reader.pending(), 4);
    // This connector-level test simulates the committed receipt; real durable
    // checkpoint authorization is tested separately through Supervisor.
    reader.checkpoint_committed(3).await.unwrap();
    assert_eq!(reader.pending(), 1);
    let other_owner = MemoryOwner::new(ResourceBudget::compact());
    let other = broker.connect(&other_owner).await;
    let refused = Reader::open(
        other,
        config.clone(),
        other_owner.clone(),
        [8; 32],
        [2; 16],
        None,
    )
    .await;
    assert!(refused
        .err()
        .unwrap()
        .message
        .contains("another checkpoint owner"));
    assert_eq!(other_owner.usage().physical_bytes, 0);
    reader.verify().await.unwrap();
    drop(js);
    reader.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
    let connection = broker.connect(&owner).await;
    let mut restored = Reader::open(
        connection,
        config,
        owner.clone(),
        [7; 32],
        [3; 16],
        saved.as_ref(),
    )
    .await
    .unwrap();
    assert!(restored.prepare_pull().await.unwrap());
    let ReaderPoll::Record(record) = restored.next().await.unwrap() else {
        panic!("uncommitted suffix missing")
    };
    assert_eq!(record.sequence(), 4);
    drop(record);
    restored.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; header expansion and retained frame test"]
async fn k2_headers_and_retained_sdk_bytes_keep_credit_after_reader_close() {
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    js.create_stream(stream_config("INPUT")).await.unwrap();
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: "OWNERS".into(),
        history: 1,
        max_bytes: 1024 * 1024,
        max_value_size: 1024,
        storage: stream::StorageType::File,
        num_replicas: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let mut headers = async_nats::HeaderMap::new();
    for i in 0..200 {
        headers.insert(format!("K{i}").as_str(), "v");
    }
    js.publish_with_headers("INPUT.rows", headers, r#"{"v":1}"#.into())
        .await
        .unwrap()
        .await
        .unwrap();
    let config = ReaderConfig {
        namespace: "test_account".into(),
        stream: "INPUT".into(),
        consumer: "headers".into(),
        ownership_bucket: "OWNERS".into(),
        max_pending: 8,
        pending_bytes: 256 * 1024,
        pull_messages: 4,
        pull_bytes: 72 * 1024,
        payload_format: Default::default(),
    };
    let mut reader = Reader::open(connection, config, owner.clone(), [4; 32], [4; 16], None)
        .await
        .unwrap();
    reader.prepare_pull().await.unwrap();
    let ReaderPoll::Record(record) = reader.next().await.unwrap() else {
        panic!("header message missing")
    };
    assert!(
        reader.pending_bytes() > record.payload().len() + 1000,
        "pending byte budget must include original headers"
    );
    drop(js);
    reader.close().await.unwrap();
    assert!(
        owner.usage().physical_bytes >= 256 * 1024,
        "retained Bytes view and decoded headers must not become uncharged at reader shutdown"
    );
    let schema = Arc::new(
        sparrow_model::Schema::new(
            1,
            vec![sparrow_model::Field::new(
                1,
                "v",
                sparrow_model::DataType::Int64,
                false,
            )],
        )
        .unwrap(),
    );
    assert_eq!(
        record.decode(&schema, &owner, 4096).unwrap().rows()[0].values[0],
        sparrow_model::Scalar::Int64(1)
    );
    drop(record);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; payload format bound into the cut"]
async fn k2_restore_refuses_changed_payload_format_or_csv_options() {
    use sparrow_formats::{CsvOptions, CsvRole, PayloadFormat};
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    js.create_stream(stream_config("INPUT")).await.unwrap();
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: "OWNERS".into(),
        history: 1,
        max_bytes: 1024 * 1024,
        max_value_size: 1024,
        storage: stream::StorageType::File,
        num_replicas: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    for n in 1..=4 {
        js.publish("INPUT.rows", format!("id,v\na,{n}\n").into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    drop(js);
    connection.close().await.unwrap();
    let csv = |options: CsvOptions| PayloadFormat::csv(options.compile(CsvRole::Decode).unwrap());
    let config = |consumer: &str, payload_format: PayloadFormat| ReaderConfig {
        namespace: "test_account".into(),
        stream: "INPUT".into(),
        consumer: consumer.into(),
        ownership_bucket: "OWNERS".into(),
        max_pending: 8,
        pending_bytes: 256 * 1024,
        pull_messages: 4,
        pull_bytes: 72 * 1024,
        payload_format,
    };
    let schema = Arc::new(
        sparrow_model::Schema::new(
            1,
            vec![
                sparrow_model::Field::new(1, "id", sparrow_model::DataType::Utf8, false),
                sparrow_model::Field::new(2, "v", sparrow_model::DataType::Int64, false),
            ],
        )
        .unwrap(),
    );
    // Read `upto` records with `cfg`, publish them and return the cut.
    async fn cut_after(
        broker: &Broker,
        owner: &Arc<MemoryOwner>,
        cfg: ReaderConfig,
        schema: &Arc<sparrow_model::Schema>,
        upto: u64,
    ) -> sparrow_io::SourcePosition {
        let csv = cfg.payload_format.as_csv().is_some();
        let mut reader = Reader::open(
            broker.connect(owner).await,
            cfg,
            owner.clone(),
            [7; 32],
            [1; 16],
            None,
        )
        .await
        .unwrap();
        assert!(reader.prepare_pull().await.unwrap());
        for n in 1..=upto {
            let ReaderPoll::Record(record) = reader.next().await.unwrap() else {
                panic!("record {n}")
            };
            assert_eq!(record.sequence(), n);
            if csv {
                let batch = record.decode(schema, owner, 4096).unwrap();
                assert_eq!(
                    batch.rows()[0].values[1],
                    sparrow_model::Scalar::Int64(n as i64)
                );
            }
            drop(record);
            reader.published(n).unwrap();
        }
        let cut = reader.position(upto);
        reader.close().await.unwrap();
        cut
    }
    async fn reopen(
        broker: &Broker,
        owner: &Arc<MemoryOwner>,
        cfg: ReaderConfig,
        cut: &sparrow_io::SourcePosition,
        nonce: u8,
    ) -> sparrow_model::Result<Reader> {
        Reader::open(
            broker.connect(owner).await,
            cfg,
            owner.clone(),
            [7; 32],
            [nonce; 16],
            Some(cut),
        )
        .await
    }

    // JSON cut -> CSV reader: refused. JSON keeps its historical identity.
    let json_cut = cut_after(
        &broker,
        &owner,
        config("json", PayloadFormat::Json),
        &schema,
        1,
    )
    .await;
    assert_eq!(json_cut.identity.fingerprint, 0, "JSON identity unchanged");
    let refused = reopen(
        &broker,
        &owner,
        config("json", csv(Default::default())),
        &json_cut,
        2,
    )
    .await
    .err()
    .expect("JSON cut must not restore as CSV");
    assert_eq!(refused.code, sparrow_model::ErrorCode::UnsupportedRestore);
    assert!(
        refused.message.contains("payload format"),
        "{}",
        refused.message
    );
    assert_eq!(owner.usage().physical_bytes, 0);

    // CSV cut -> JSON reader or other CSV options: refused.
    let cut = cut_after(
        &broker,
        &owner,
        config("csv", csv(Default::default())),
        &schema,
        2,
    )
    .await;
    assert_ne!(cut.identity.fingerprint, 0);
    for (nonce, other) in [
        (3, PayloadFormat::Json),
        (
            4,
            csv(CsvOptions {
                trim: true,
                ..Default::default()
            }),
        ),
        (
            5,
            csv(CsvOptions {
                null_value: "NULL".into(),
                ..Default::default()
            }),
        ),
        (
            6,
            csv(CsvOptions {
                max_record_bytes: Some(1024),
                ..Default::default()
            }),
        ),
    ] {
        let refused = reopen(&broker, &owner, config("csv", other.clone()), &cut, nonce)
            .await
            .err()
            .unwrap_or_else(|| panic!("{other:?} must be refused"));
        assert_eq!(refused.code, sparrow_model::ErrorCode::UnsupportedRestore);
        assert!(
            refused.message.contains("payload format"),
            "{}",
            refused.message
        );
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    // The same options, spelled with explicit defaults, restore exactly after
    // the cut.
    let explicit = csv(CsvOptions {
        max_record_bytes: Some(65536),
        max_fields: Some(256),
        ..Default::default()
    });
    let mut restored = reopen(&broker, &owner, config("csv", explicit), &cut, 7)
        .await
        .unwrap();
    assert!(restored.prepare_pull().await.unwrap());
    for n in 3..=4 {
        let ReaderPoll::Record(record) = restored.next().await.unwrap() else {
            panic!("suffix record {n}")
        };
        assert_eq!(record.sequence(), n);
        let batch = record.decode(&schema, &owner, 4096).unwrap();
        assert_eq!(
            batch.rows()[0].values[1],
            sparrow_model::Scalar::Int64(n as i64)
        );
    }
    restored.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; protobuf identity bound into the cut"]
async fn k2_restore_refuses_changed_protobuf_descriptor_message_mapping_or_policy() {
    use base64::Engine;
    use sparrow_formats::{CsvOptions, CsvRole, PayloadFormat, ProtobufOptions, UnknownFields};
    const DESCRIPTOR: &[u8] =
        include_bytes!("../../../sparrow-formats/tests/fixtures/protobuf/descriptor_set.pb");
    let broker = Broker::start().await;
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let connection = broker.connect(&owner).await;
    let js = async_nats::jetstream::new(connection.client.clone());
    js.create_stream(stream_config("INPUT")).await.unwrap();
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: "OWNERS".into(),
        history: 1,
        max_bytes: 1024 * 1024,
        max_value_size: 1024,
        storage: stream::StorageType::File,
        num_replicas: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    for n in 1..=4u8 {
        // telemetry.v1.Reading { device: "a", seq: n }
        js.publish("INPUT.rows", vec![0x0a, 0x01, b'a', 0x10, n].into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    drop(js);
    connection.close().await.unwrap();
    let base = ProtobufOptions {
        descriptor_set: base64::engine::general_purpose::STANDARD.encode(DESCRIPTOR),
        message: "telemetry.v1.Reading".into(),
        fields: [("id", "device"), ("v", "seq")]
            .iter()
            .map(|(c, p)| (c.to_string(), p.to_string()))
            .collect(),
        unknown_fields: UnknownFields::Ignore,
        max_message_bytes: None,
        max_depth: None,
    };
    let protobuf = |tune: &dyn Fn(&mut ProtobufOptions)| {
        let mut options = base.clone();
        tune(&mut options);
        PayloadFormat::protobuf(options.compile(CsvRole::Decode).unwrap())
    };
    let config = |payload_format: PayloadFormat| ReaderConfig {
        namespace: "test_account".into(),
        stream: "INPUT".into(),
        consumer: "pb".into(),
        ownership_bucket: "OWNERS".into(),
        max_pending: 8,
        pending_bytes: 256 * 1024,
        pull_messages: 4,
        pull_bytes: 72 * 1024,
        payload_format,
    };
    let schema = Arc::new(
        sparrow_model::Schema::new(
            1,
            vec![
                sparrow_model::Field::new(1, "id", sparrow_model::DataType::Utf8, false),
                sparrow_model::Field::new(2, "v", sparrow_model::DataType::Int64, false),
            ],
        )
        .unwrap(),
    );
    let mut reader = Reader::open(
        broker.connect(&owner).await,
        config(protobuf(&|_| {})),
        owner.clone(),
        [7; 32],
        [1; 16],
        None,
    )
    .await
    .unwrap();
    assert!(reader.prepare_pull().await.unwrap());
    for n in 1..=2u64 {
        let ReaderPoll::Record(record) = reader.next().await.unwrap() else {
            panic!("record {n}")
        };
        assert_eq!(record.sequence(), n);
        let batch = record.decode(&schema, &owner, 4096).unwrap();
        assert_eq!(
            batch.rows()[0].values,
            vec![
                sparrow_model::Scalar::utf8("a"),
                sparrow_model::Scalar::Int64(n as i64)
            ]
        );
        drop(record);
        reader.published(n).unwrap();
    }
    let cut = reader.position(2);
    reader.close().await.unwrap();
    assert_ne!(cut.identity.fingerprint, 0);

    let others: Vec<(&str, PayloadFormat)> = vec![
        ("json", PayloadFormat::Json),
        (
            "csv",
            PayloadFormat::csv(CsvOptions::default().compile(CsvRole::Decode).unwrap()),
        ),
        ("message", protobuf(&|o| o.message = "telemetry.v1.Tree".into())),
        ("mapping", protobuf(&|o| {
            o.fields.insert("v".into(), "i32".into());
        })),
        ("unknown policy", protobuf(&|o| o.unknown_fields = UnknownFields::Error)),
        ("max_depth", protobuf(&|o| o.max_depth = Some(31))),
        ("max_message_bytes", protobuf(&|o| o.max_message_bytes = Some(1024))),
        // Same message types, a descriptor set without legacy.proto.
        ("descriptor bytes", protobuf(&|o| {
            o.descriptor_set = base64::engine::general_purpose::STANDARD.encode(include_bytes!(
                "../../../sparrow-formats/tests/fixtures/protobuf/descriptor_set_telemetry.pb"
            ))
        })),
    ];
    for (nonce, (what, other)) in others.into_iter().enumerate() {
        let refused = Reader::open(
            broker.connect(&owner).await,
            config(other),
            owner.clone(),
            [7; 32],
            [nonce as u8 + 2; 16],
            Some(&cut),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{what} change must be refused"));
        assert_eq!(refused.code, sparrow_model::ErrorCode::UnsupportedRestore, "{what}");
        assert!(refused.message.contains("payload format"), "{what}: {}", refused.message);
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    // Explicit defaults are the same identity (same-name mapping entries:
    // sparrow-formats unit tests).
    let explicit = protobuf(&|o| {
        o.max_depth = Some(32);
        o.max_message_bytes = Some(65536);
    });
    let mut restored = Reader::open(
        broker.connect(&owner).await,
        config(explicit),
        owner.clone(),
        [7; 32],
        [42; 16],
        Some(&cut),
    )
    .await
    .unwrap();
    assert!(restored.prepare_pull().await.unwrap());
    for n in 3..=4u64 {
        let ReaderPoll::Record(record) = restored.next().await.unwrap() else {
            panic!("suffix record {n}")
        };
        assert_eq!(record.sequence(), n);
        let batch = record.decode(&schema, &owner, 4096).unwrap();
        assert_eq!(batch.rows()[0].values[1], sparrow_model::Scalar::Int64(n as i64));
        drop(batch);
        drop(record);
    }
    restored.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

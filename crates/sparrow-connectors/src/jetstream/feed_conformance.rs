//! JetStream feed-observation conformance on the pinned real broker.
//!
//! Opt-in (`SPARROW_NATS_SERVER`) and isolated: only the fixture's own child
//! server is started and stopped. An observation is a point-in-time prefix fact
//! about this reader, never device health, and these tests make no availability
//! or certification claim.

use super::*;
use sparrow_io::feed::FeedReadiness;

/// Mirrors `reader::reader_name`: the durable consumer name this fixture's
/// reader creates for `base` under `nonce`.
fn reader_consumer_name(base: &str, nonce: [u8; 16]) -> String {
    use std::fmt::Write;
    let mut name = format!("{base}_");
    for byte in nonce {
        write!(name, "{byte:02x}").expect("String write");
    }
    name
}

fn feed_config(consumer: &str) -> ReaderConfig {
    ReaderConfig {
        namespace: "test_account".into(),
        stream: "INPUT".into(),
        consumer: consumer.into(),
        ownership_bucket: "OWNERS".into(),
        max_pending: 8,
        pending_bytes: 256 * 1024,
        pull_messages: 4,
        pull_bytes: 72 * 1024,
        payload_format: Default::default(),
    }
}

/// Isolated broker plus an opened reader for one logical consumer name.
async fn feed_fixture(
    consumer: &str,
    nonce: [u8; 16],
    owner: &Arc<MemoryOwner>,
) -> (Broker, Reader) {
    let broker = Broker::start().await;
    let connection = broker.connect(owner).await;
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
    let reader = Reader::open(
        connection,
        feed_config(consumer),
        owner.clone(),
        [7; 32],
        nonce,
        None,
    )
    .await
    .unwrap();
    (broker, reader)
}

/// A probe must leave every local progress position untouched. The published
/// cut is read through `position` (`published` is a setter only).
fn progress(reader: &Reader) -> (u64, u64, u64) {
    (
        reader.position(0).offset_bytes,
        reader.committed(),
        reader.pulls(),
    )
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; isolated broker child only"]
async fn k2_feed_observation_reports_the_fresh_prefix_without_advancing_progress() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (broker, mut reader) = feed_fixture("feed_progress", [5; 16], &owner).await;
    let admin = async_nats::connect(format!("nats://127.0.0.1:{}", broker.port))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(admin);

    // An empty stream with a freshly created consumer is caught up.
    let start = progress(&reader);
    let empty = reader.observe_feed(0).await.unwrap();
    assert_eq!(empty.readiness, FeedReadiness::CaughtUp);
    assert_eq!((empty.head, empty.position.offset_bytes), (0, 0));
    assert_eq!(
        progress(&reader),
        start,
        "observation must not touch progress"
    );

    // Another publisher's input is a known server backlog, never silence.
    for n in 1..=3 {
        js.publish("INPUT.rows", format!("{{\"v\":{n}}}").into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let backlog = reader.observe_feed(0).await.unwrap();
    assert_eq!(backlog.readiness, FeedReadiness::Backlog);
    assert_eq!(backlog.head, 3);
    assert_eq!(progress(&reader), start);

    // An admitted but unpublished delivery is in flight, and the head stays
    // the fresh stream value.
    assert!(reader.prepare_pull().await.unwrap());
    let ReaderPoll::Record(first) = reader.next().await.unwrap() else {
        panic!("expected the first row")
    };
    assert_eq!(first.sequence(), 1);
    let admitted = reader.observe_feed(0).await.unwrap();
    assert_eq!(admitted.readiness, FeedReadiness::InFlight);
    assert_eq!(admitted.head, 3);

    // The fetch is still open: the reader is mid-batch, so even a head ahead
    // of the published prefix is in flight rather than a quiet backlog.
    reader.published(1).unwrap();
    drop(first);
    let open_fetch = reader.observe_feed(1).await.unwrap();
    assert_eq!(open_fetch.readiness, FeedReadiness::InFlight);
    assert_eq!(open_fetch.head, 3);
    assert_eq!(open_fetch.position.offset_bytes, 1);

    // Drain the fetch to its end, which closes the reader's pull.
    for n in 2..=3 {
        let ReaderPoll::Record(record) = reader.next().await.unwrap() else {
            panic!("row {n} missing from the open fetch")
        };
        assert_eq!(record.sequence(), n);
        reader.published(n).unwrap();
    }
    assert!(matches!(reader.next().await.unwrap(), ReaderPoll::BatchEnd));
    assert_eq!(reader.pending(), 3);

    // With no pull open, a later publisher write is a real backlog.
    let before = progress(&reader);
    js.publish("INPUT.rows", "{\"v\":4}".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let later = reader.observe_feed(3).await.unwrap();
    assert_eq!(later.readiness, FeedReadiness::Backlog);
    assert_eq!((later.head, later.position.offset_bytes), (4, 3));
    assert_eq!(progress(&reader), before);

    // Fetch the remaining row, then run a fetch that returns nothing at all.
    assert!(reader.prepare_pull().await.unwrap());
    let ReaderPoll::Record(last) = reader.next().await.unwrap() else {
        panic!("the published row must be delivered")
    };
    assert_eq!(last.sequence(), 4);
    reader.published(4).unwrap();
    drop(last);
    assert!(matches!(reader.next().await.unwrap(), ReaderPoll::BatchEnd));
    assert!(reader.prepare_pull().await.unwrap());
    assert!(
        matches!(reader.next().await.unwrap(), ReaderPoll::Empty),
        "a drained fetch must not invent frames"
    );

    // Unconfirmed ACKs are not a readiness failure: the published prefix is
    // drained by the head and the consumer counters, not by an ACK count.
    assert_eq!(reader.pending(), 4);
    let unacked = reader.observe_feed(4).await.unwrap();
    assert_eq!(unacked.readiness, FeedReadiness::CaughtUp);
    assert_eq!(unacked.head, 4);
    reader.checkpoint_committed(4).await.unwrap();
    assert_eq!(reader.pending(), 0);
    let settled = reader.observe_feed(4).await.unwrap();
    assert_eq!(settled.readiness, FeedReadiness::CaughtUp);
    assert_eq!(settled.position.offset_bytes, 4);
    assert_eq!(settled.position.record_index, 4);

    // A later publisher write is visible on the next probe, still without
    // mutating this reader's own positions.
    let stable = progress(&reader);
    js.publish("INPUT.rows", "{\"v\":5}".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let again = reader.observe_feed(4).await.unwrap();
    assert_eq!(again.readiness, FeedReadiness::Backlog);
    assert_eq!(again.head, 5);
    assert_eq!(progress(&reader), stable);

    // Recovery boundary: reopen at the saved published cut with a new attempt
    // nonce on the same broker, stream, and binding owner. The restarted
    // consumer counts its deliveries from 0/1 while the stream offset
    // continues from the cut, so a correct probe must not report a regression
    // and must still see the row beyond the cut as a backlog.
    let saved = reader.position(4);
    reader.close().await.unwrap();
    let connection = broker.connect(&owner).await;
    let mut restored = Reader::open(
        connection,
        feed_config("feed_progress"),
        owner.clone(),
        [7; 32],
        [6; 16],
        Some(&saved),
    )
    .await
    .unwrap();
    let resumed = restored.observe_feed(4).await.unwrap();
    assert_eq!(resumed.readiness, FeedReadiness::Backlog);
    assert_eq!((resumed.head, resumed.position.offset_bytes), (5, 4));
    assert_eq!(progress(&restored), (4, 4, 0));
    assert!(restored.prepare_pull().await.unwrap());
    let ReaderPoll::Record(record) = restored.next().await.unwrap() else {
        panic!("the restored reader must drain the row beyond its cut")
    };
    assert_eq!(record.sequence(), 5);
    restored.published(5).unwrap();
    drop(record);
    assert!(matches!(
        restored.next().await.unwrap(),
        ReaderPoll::BatchEnd
    ));
    let drained = restored.observe_feed(5).await.unwrap();
    assert_eq!(drained.readiness, FeedReadiness::CaughtUp);
    assert_eq!((drained.head, drained.position.offset_bytes), (5, 5));

    drop(js);
    restored.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; consumer identity/policy mutation"]
async fn k2_feed_observation_refuses_consumer_config_and_incarnation_changes() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (broker, mut reader) = feed_fixture("feed_policy", [6; 16], &owner).await;
    let admin = async_nats::connect(format!("nats://127.0.0.1:{}", broker.port))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(admin);
    let stream = js.get_stream("INPUT").await.unwrap();
    let name = reader_consumer_name("feed_policy", [6; 16]);
    assert_eq!(
        reader.observe_feed(0).await.unwrap().readiness,
        FeedReadiness::CaughtUp
    );

    // Same incarnation, changed mutable configuration: the probe must not keep
    // claiming this feed under the original consumer identity.
    let existing = stream.get_consumer::<pull::Config>(&name).await.unwrap();
    let mut changed = existing.cached_info().config.clone();
    changed.ack_wait = Duration::from_secs(11);
    stream.update_consumer(changed).await.unwrap();
    let error = reader.observe_feed(0).await.unwrap_err();
    assert_eq!(
        error.code,
        sparrow_model::ErrorCode::PolicyDenied,
        "a reconfigured consumer must not pass as the original feed"
    );
    assert_eq!(progress(&reader), (0, 0, 0));
    reader.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);

    // A durable consumer deleted and recreated under the same name is a new
    // incarnation even when its configuration is identical.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (broker, mut reader) = feed_fixture("feed_incarnation", [7; 16], &owner).await;
    let admin = async_nats::connect(format!("nats://127.0.0.1:{}", broker.port))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(admin);
    let stream = js.get_stream("INPUT").await.unwrap();
    let name = reader_consumer_name("feed_incarnation", [7; 16]);
    assert_eq!(
        reader.observe_feed(0).await.unwrap().readiness,
        FeedReadiness::CaughtUp
    );
    let config = stream
        .get_consumer::<pull::Config>(&name)
        .await
        .unwrap()
        .cached_info()
        .config
        .clone();
    stream.delete_consumer(&name).await.unwrap();
    stream.create_consumer_strict(config).await.unwrap();
    let error = reader.observe_feed(0).await.unwrap_err();
    assert_eq!(
        error.code,
        sparrow_model::ErrorCode::PolicyDenied,
        "a recreated consumer must not pass as the original feed"
    );
    assert_eq!(progress(&reader), (0, 0, 0));
    reader.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[tokio::test]
#[ignore = "requires SPARROW_NATS_SERVER; ownership change and disconnect"]
async fn k2_feed_observation_refuses_ownership_change_and_disconnect() {
    // A reader that no longer holds the durable ownership binding is refused
    // even though its consumer and stream are untouched.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (broker, mut reader) = feed_fixture("feed_owner", [8; 16], &owner).await;
    let admin = async_nats::connect(format!("nats://127.0.0.1:{}", broker.port))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(admin);
    assert_eq!(
        reader.observe_feed(0).await.unwrap().readiness,
        FeedReadiness::CaughtUp
    );
    let kv = js.get_key_value("OWNERS").await.unwrap();
    let binding = kv.entry("INPUT.feed_owner").await.unwrap().unwrap();
    let mut forged = Vec::with_capacity(52);
    forged.extend_from_slice(b"BND1");
    forged.extend_from_slice(&[1; 32]);
    forged.extend_from_slice(&[2; 16]);
    kv.update("INPUT.feed_owner", forged.into(), binding.revision)
        .await
        .unwrap();
    let error = reader.observe_feed(0).await.unwrap_err();
    assert_eq!(error.code, sparrow_model::ErrorCode::PolicyDenied);
    assert_eq!(progress(&reader), (0, 0, 0));
    reader.close().await.unwrap();
    assert_eq!(owner.usage().physical_bytes, 0);

    // A broker that goes away must fail the probe, never look caught up.
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let (mut broker, mut reader) = feed_fixture("feed_disconnect", [9; 16], &owner).await;
    assert_eq!(
        reader.observe_feed(0).await.unwrap().readiness,
        FeedReadiness::CaughtUp
    );
    broker.stop();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let error = reader.observe_feed(0).await.unwrap_err();
    assert!(!error.message.is_empty());
    assert_eq!(progress(&reader), (0, 0, 0));
    // A transport error cannot silently strand the probe's SDK/ledger credit.
    let closed = reader.close().await;
    assert_eq!(owner.usage().physical_bytes, 0, "close result: {closed:?}");
}

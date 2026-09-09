use std::time::Duration;

use sparrow_connectors::{
    publish_qos0, sensor_json, EmbeddedBroker, IoDiagnostics, MapSecretResolver, MqttSource,
    MqttSourceConfig, TargetPolicy,
};
use sparrow_model::Scalar;
use sparrow_testkit::sensor_schema;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn broker_source_decodes_json() {
    let broker = EmbeddedBroker::start().await.unwrap();
    let policy = TargetPolicy::allow(broker.host(), broker.port());
    let secrets = MapSecretResolver::empty();
    let diag = IoDiagnostics::new();
    let cfg = MqttSourceConfig::demo(broker.host(), broker.port(), sensor_schema());
    let source = MqttSource::bind(cfg, &secrets, &policy, diag.clone()).unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(source.run(tx, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(80)).await;
    publish_qos0(
        &broker.host(),
        broker.port(),
        "pub",
        "sensors/json",
        sensor_json("edge-a", 26.2, 39.0, 1_700_000_001_000_000, false),
    )
    .await
    .unwrap();
    let row = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(row.values[0], Scalar::utf8("edge-a"));
    cancel.cancel();
    let _ = task.await;
    broker.stop().await;
}

#[tokio::test]
async fn http_sink_posts_to_capture() {
    use sparrow_connectors::{HttpCapture, HttpSink, HttpSinkConfig};
    use sparrow_formats::encode_json_row;
    use sparrow_model::{CreditKind, MemoryOwner, ResourceBudget, RowBatchBuilder};
    use sparrow_testkit::{sensor_fixture, sensor_schema};
    use std::sync::Arc;

    let http = HttpCapture::start().await.unwrap();
    let policy = TargetPolicy::allow("127.0.0.1", http.port());
    let secrets = MapSecretResolver::empty();
    let diag = IoDiagnostics::new();
    let sink = HttpSink::bind(
        HttpSinkConfig::demo(http.url()),
        &secrets,
        &policy,
        diag,
    )
    .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(sink.run(rx, cancel.clone()));

    let schema = Arc::new(sensor_schema());
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let rec = sensor_fixture().into_iter().nth(1).unwrap();
    let mut b = RowBatchBuilder::new(
        Arc::clone(&schema),
        owner,
        CreditKind::Reservation,
        1,
        64 * 1024,
    )
    .unwrap();
    b.push(rec.to_row()).unwrap();
    let batch = b.finish().unwrap();
    let expected = encode_json_row(&schema, &batch.rows()[0]).unwrap();
    tx.send(batch).await.unwrap();
    drop(tx);

    let start = std::time::Instant::now();
    loop {
        if http.bodies().len() == 1 {
            break;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("HTTP capture empty");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(http.bodies()[0], expected);
    cancel.cancel();
    let _ = task.await;
    http.stop().await;
}

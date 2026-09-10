//! HTTP Push Source → kernel → MQTT Sink, with an embedded broker and a subscriber.

use std::sync::Arc;
use std::time::Duration;

use sparrow_cli::{compact_kernel, hot_sensor_plan};
use sparrow_connectors::{
    publish_qos0, EmbeddedBroker, HttpPushSource, HttpPushSourceConfig, MapSecretResolver, MqttSink,
    MqttSinkConfig, MqttSource, MqttSourceConfig, TargetPolicy,
};
use sparrow_model::{ErrorCode, Row};
use sparrow_runtime::{JobRequest, SharedCapture};
use sparrow_testkit::sensor_schema;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn main() {
    if let Err(e) = run() {
        eprintln!("v02_http_mqtt_loop failed: {e}");
        std::process::exit(1);
    }
}

fn run() -> sparrow_model::Result<()> {
    println!("=== Sparrow V0.2 HTTP Push → kernel → MQTT Sink ===");
    let kernel = compact_kernel()?;
    kernel.block_on(run_async(&kernel))
}

async fn run_async(kernel: &sparrow_runtime::Kernel) -> sparrow_model::Result<()> {
    let broker = EmbeddedBroker::start().await.map_err(sparrow_model::SparrowError::from)?;
    let secrets = MapSecretResolver::empty();
    let policy = TargetPolicy::allow(broker.host(), broker.port()).with_allow("127.0.0.1", 0);
    let diag = Arc::new(sparrow_connectors::IoDiagnostics::new());

    let push_cfg = HttpPushSourceConfig::demo(sensor_schema());
    let push = HttpPushSource::bind(push_cfg, &secrets, &policy, Arc::clone(&diag))
        .await
        .map_err(sparrow_model::SparrowError::from)?;
    let push_url = push.url();
    println!("http_push {}", push_url);
    println!("mqtt broker {}:{}", broker.host(), broker.port());

    let mut sink_cfg = MqttSinkConfig::demo(broker.host(), broker.port());
    sink_cfg.topic = "sparrow/out".into();
    sink_cfg.client_id = "v02-sink".into();
    let sink = MqttSink::bind(sink_cfg, &secrets, &policy, Arc::clone(&diag))
        .map_err(sparrow_model::SparrowError::from)?;

    let mut sub_cfg = MqttSourceConfig::demo(broker.host(), broker.port(), sensor_schema());
    sub_cfg.topic = "sparrow/out".into();
    sub_cfg.client_id = "v02-sub".into();
    let sub = MqttSource::bind(sub_cfg, &secrets, &policy, Arc::clone(&diag))
        .map_err(sparrow_model::SparrowError::from)?;

    let (tx_in, rx_in) = tokio::sync::mpsc::channel::<Row>(16);
    let (tx_out, rx_out) = tokio::sync::mpsc::channel(16);
    let (tx_sub, mut rx_sub) = tokio::sync::mpsc::channel::<Row>(16);
    let capture = SharedCapture::new();
    let job = kernel.submit(
        JobRequest::new(hot_sensor_plan()?, Vec::new(), capture.clone()).with_live_io(rx_in, tx_out),
    )?;
    let cancel = job.cancellation();
    let push_task = kernel.handle().spawn(push.run(tx_in, cancel.clone()));
    let sink_task = kernel.handle().spawn(sink.run(rx_out, cancel.clone(), None));
    let sub_task = kernel.handle().spawn(sub.run(tx_sub, cancel.clone()));
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Cold row (18.5) should be filtered; hot rows pass.
    http_post(&push_url, br#"{"device_id":"edge-a","temperature":18.5,"humidity":40.0,"ts":1700000000000000}"#).await?;
    http_post(&push_url, br#"{"device_id":"edge-a","temperature":26.2,"humidity":39.0,"ts":1700000001000000}"#).await?;
    http_post(&push_url, br#"{"device_id":"edge-b","temperature":31.0,"humidity":55.0,"ts":1700000002000000}"#).await?;

    let mut seen = Vec::new();
    let start = std::time::Instant::now();
    while seen.len() < 2 && start.elapsed() < Duration::from_secs(5) {
        tokio::select! {
            row = rx_sub.recv() => {
                if let Some(r) = row {
                    seen.push(r);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
    println!("mqtt subscriber saw {} filtered rows", seen.len());
    if seen.len() < 2 {
        return Err(sparrow_model::SparrowError::new(
            ErrorCode::Internal,
            format!("expected >=2 MQTT sink rows, got {}", seen.len()),
        ));
    }

    job.stop().await?;
    let _ = push_task.await;
    let _ = sink_task.await;
    let _ = sub_task.await;
    broker.stop().await;
    let _ = publish_qos0;
    println!("v02_http_mqtt_loop: ok");
    Ok(())
}

async fn http_post(url: &str, body: &[u8]) -> sparrow_model::Result<()> {
    let rest = url.strip_prefix("http://").unwrap_or(url);
    let (hostport, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = hostport.split_once(':').unwrap();
    let mut s = TcpStream::connect((host, port.parse::<u16>().unwrap()))
        .await
        .map_err(|e| sparrow_model::SparrowError::new(ErrorCode::Internal, e.to_string()))?;
    let req = format!(
        "POST /{path} HTTP/1.1\r\nhost: {hostport}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(req.as_bytes())
        .await
        .map_err(|e| sparrow_model::SparrowError::new(ErrorCode::Internal, e.to_string()))?;
    s.write_all(body)
        .await
        .map_err(|e| sparrow_model::SparrowError::new(ErrorCode::Internal, e.to_string()))?;
    let mut buf = [0u8; 256];
    let _ = s.read(&mut buf).await;
    Ok(())
}

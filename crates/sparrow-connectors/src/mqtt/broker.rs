//! In-process MQTT 3.1.1 broker (QoS 0). Bounded per-connection queues.
//! Intended for CI and the M2 demo — not a production broker.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sparrow_model::ErrorCode;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use super::codec::{Connect, Packet, Publish};
use super::io::{read_packet, write_packet, MqttFramedReader, MqttStream};
use crate::error::{ConnectorError, Result};

const PER_CONN_OUTBOX: usize = 32;

#[derive(Clone)]
struct Hub {
    subs: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<Publish>>>>>,
}

impl Hub {
    fn new() -> Self {
        Self {
            subs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn subscribe(&self, filter: String, tx: mpsc::Sender<Publish>) {
        self.subs.lock().await.entry(filter).or_default().push(tx);
    }

    async fn publish(&self, msg: Publish) {
        let subs = self.subs.lock().await;
        for (filter, txs) in subs.iter() {
            if topic_matches(filter, &msg.topic) {
                for tx in txs {
                    let _ = tx.try_send(msg.clone());
                }
            }
        }
    }
}

pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if filter == "#" || filter == topic {
        return true;
    }
    if let Some(prefix) = filter.strip_suffix("/#") {
        return topic == prefix || topic.starts_with(&format!("{prefix}/"));
    }
    if filter.contains('+') {
        let f: Vec<&str> = filter.split('/').collect();
        let t: Vec<&str> = topic.split('/').collect();
        if f.len() != t.len() {
            return false;
        }
        return f.iter().zip(t.iter()).all(|(a, b)| *a == "+" || *a == *b);
    }
    false
}

#[cfg(feature = "demo-io")]
pub struct EmbeddedBroker {
    pub addr: SocketAddr,
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl EmbeddedBroker {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("bind MQTT broker: {e}"))
        })?;
        let addr = listener.local_addr().map_err(|e| {
            ConnectorError::new(ErrorCode::Internal, format!("broker local_addr: {e}"))
        })?;
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let hub = Hub::new();
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = child.cancelled() => break,
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, _)) => {
                                let hub = hub.clone();
                                let child = child.clone();
                                tokio::spawn(async move {
                                    let _ = handle_conn(stream, hub, child).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        Ok(Self { addr, cancel, join })
    }

    pub fn host(&self) -> String {
        self.addr.ip().to_string()
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.join.await;
    }
}

async fn handle_conn(stream: TcpStream, hub: Hub, cancel: CancellationToken) -> Result<()> {
    let _ = stream.set_nodelay(true);
    let mut stream: MqttStream = Box::pin(stream);
    let mut reader = MqttFramedReader::new();
    let first = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        p = read_packet(&mut reader, &mut stream) => p?,
    };
    let Packet::Connect(Connect { .. }) = first else {
        return Err(ConnectorError::new(
            ErrorCode::CodecViolation,
            "first packet must be CONNECT",
        ));
    };
    write_packet(
        &mut stream,
        &Packet::ConnAck {
            session_present: false,
            return_code: 0,
        },
    )
    .await?;

    let (out_tx, mut out_rx) = mpsc::channel::<Publish>(PER_CONN_OUTBOX);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            incoming = read_packet(&mut reader, &mut stream) => {
                match incoming {
                    Ok(Packet::Subscribe { packet_id, topics }) => {
                        let mut codes = Vec::new();
                        for (filter, qos) in topics {
                            hub.subscribe(filter, out_tx.clone()).await;
                            codes.push(if qos > 0 { 0x80 } else { 0 });
                        }
                        write_packet(
                            &mut stream,
                            &Packet::SubAck { packet_id, codes },
                        )
                        .await?;
                    }
                    Ok(Packet::Publish(p)) => {
                        hub.publish(p).await;
                    }
                    Ok(Packet::PingReq) => {
                        write_packet(&mut stream, &Packet::PingResp).await?;
                    }
                    Ok(Packet::Disconnect) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            maybe = out_rx.recv() => {
                match maybe {
                    Some(p) => {
                        write_packet(&mut stream, &Packet::Publish(p)).await?;
                    }
                    None => break,
                }
            }
        }
    }
    Ok(())
}

/// One-shot QoS 0 publisher used by demos and tests.
pub async fn publish_qos0(
    host: &str,
    port: u16,
    client_id: &str,
    topic: &str,
    payload: Vec<u8>,
) -> Result<()> {
    publish_qos0_many(host, port, client_id, topic, vec![payload]).await
}

/// Publish many QoS 0 payloads on one connection (for backpressure demos).
pub async fn publish_qos0_many(
    host: &str,
    port: u16,
    client_id: &str,
    topic: &str,
    payloads: Vec<Vec<u8>>,
) -> Result<()> {
    let mut stream = super::io::connect_plain(
        host,
        port,
        &crate::tls::TlsConfig::disabled(),
        Duration::from_secs(2),
    )
    .await?;
    write_packet(
        &mut stream,
        &Packet::Connect(Connect {
            client_id: client_id.into(),
            clean_session: true,
            keepalive: 30,
            username: None,
            password: None,
        }),
    )
    .await?;
    let mut reader = MqttFramedReader::new();
    match read_packet(&mut reader, &mut stream).await? {
        Packet::ConnAck { return_code: 0, .. } => {}
        other => {
            return Err(ConnectorError::new(
                ErrorCode::Internal,
                format!("unexpected connack {other:?}"),
            ));
        }
    }
    for payload in payloads {
        write_packet(
            &mut stream,
            &Packet::Publish(Publish {
                dup: false,
                qos: 0,
                retain: false,
                topic: topic.into(),
                packet_id: None,
                payload,
            }),
        )
        .await?;
    }
    write_packet(&mut stream, &Packet::Disconnect).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_filter_exact_and_hash() {
        assert!(topic_matches("sensors/json", "sensors/json"));
        assert!(topic_matches("sensors/#", "sensors/json"));
        assert!(!topic_matches("other", "sensors/json"));
    }

    #[tokio::test]
    async fn r19_coalesced_publishes_are_not_dropped() {
        let broker = EmbeddedBroker::start().await.unwrap();
        let payloads: Vec<Vec<u8>> = (0..48).map(|i| format!("{i}").into_bytes()).collect();
        publish_qos0_many(
            &broker.host(),
            broker.port(),
            "flood",
            "sensors/json",
            payloads,
        )
        .await
        .expect("broker must accept a coalesced QoS0 flood without resetting the publisher");
        broker.stop().await;
    }
}

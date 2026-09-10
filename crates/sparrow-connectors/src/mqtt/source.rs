use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{JsonCodec, JsonLimits};
use sparrow_model::{ErrorCode, ResourceBudget, RestoreClaim, Row, Schema, SourceFrame};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::codec::{Connect, Packet, Publish};
use super::io::{connect_plain, write_packet, MqttFramedReader};
use crate::capabilities::{
    refuse_dirty_session, refuse_durable_recovery, refuse_qos_durable, ConnectorCapabilities,
};
use crate::diag::IoDiagnostics;
use crate::error::{ConnectorError, Result};
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;
use crate::tls::TlsConfig;

const MAX_INBOX: usize = 1024;
const DEFAULT_INBOX: usize = 32;

#[derive(Clone, Debug)]
pub struct MqttSourceConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub qos: u8,
    pub clean_session: bool,
    pub username_secret: Option<String>,
    pub password_secret: Option<String>,
    pub tls: TlsConfig,
    pub connect_timeout: Duration,
    pub keepalive: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub inbox_capacity: usize,
    pub restore: RestoreClaim,
    pub schema: Schema,
    pub json_limits: JsonLimits,
    /// When true, a decode error fails the source (and the job) instead of
    /// only incrementing [`IoDiagnostics::decode_errors`] (P1-17).
    pub fail_on_decode: bool,
}

impl MqttSourceConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::MQTT_SOURCE
    }

    pub fn validate(
        &self,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
    ) -> Result<()> {
        refuse_qos_durable(self.qos)?;
        refuse_dirty_session(self.clean_session)?;
        refuse_durable_recovery(&self.restore)?;
        self.tls.validate()?;
        if self.host.is_empty() || self.client_id.is_empty() || self.topic.is_empty() {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "MQTT host, client_id, and topic are required",
            ));
        }
        if self.inbox_capacity == 0 || self.inbox_capacity > MAX_INBOX {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "MQTT inbox_capacity {} is outside 1..={MAX_INBOX} (buffers are bounded)",
                    self.inbox_capacity
                ),
            ));
        }
        policy.check_host_port(&self.host, self.port)?;
        if (self.username_secret.is_some() || self.password_secret.is_some()) && !self.tls.enabled
        {
            return Err(ConnectorError::new(
                ErrorCode::PolicyDenied,
                "MQTT credentials require TLS; refusing plaintext username/password",
            ));
        }
        if let Some(name) = &self.username_secret {
            let _ = secrets.resolve(name)?;
        }
        if let Some(name) = &self.password_secret {
            let _ = secrets.resolve(name)?;
        }
        self.check_inbox_budget(ResourceBudget::compact().queue_bytes)?;
        Ok(())
    }

    pub fn inbox_worst_case_bytes(&self) -> usize {
        self.inbox_capacity
            .saturating_mul(self.json_limits.max_bytes)
    }

    /// Hard reject when `inbox_capacity × max_record` exceeds the static
    /// 4MiB ceiling **or** the process/job queue budget (P1-27).
    pub fn check_inbox_budget(&self, queue_budget: usize) -> Result<()> {
        let worst = self.inbox_worst_case_bytes();
        const STATIC: usize = 4 * 1024 * 1024;
        if worst > STATIC {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "MQTT inbox worst-case {worst}B exceeds 4MiB byte bound (inbox_capacity × max_record)"
                ),
            ));
        }
        if worst > queue_budget {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "MQTT inbox worst-case {worst}B exceeds queue budget {queue_budget}B (inbox_capacity × max_record)"
                ),
            ));
        }
        Ok(())
    }
}

impl MqttSourceConfig {
    pub fn demo(host: impl Into<String>, port: u16, schema: Schema) -> Self {
        Self {
            host: host.into(),
            port,
            client_id: "sparrow-mqtt-source".into(),
            topic: "sensors/json".into(),
            qos: 0,
            clean_session: true,
            username_secret: None,
            password_secret: None,
            tls: TlsConfig::disabled(),
            connect_timeout: Duration::from_secs(2),
            keepalive: Duration::from_secs(30),
            reconnect_min: Duration::from_millis(50),
            reconnect_max: Duration::from_secs(2),
            inbox_capacity: DEFAULT_INBOX,
            restore: RestoreClaim::None,
            schema,
            json_limits: JsonLimits::default(),
            fail_on_decode: false,
        }
    }
}

pub struct MqttSource {
    pub config: MqttSourceConfig,
    pub diag: Arc<IoDiagnostics>,
    codec: JsonCodec,
    username: Option<String>,
    password: Option<Vec<u8>>,
}

impl MqttSource {
    pub fn bind(
        config: MqttSourceConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        let username = match &config.username_secret {
            Some(n) => Some(secrets.resolve(n)?),
            None => None,
        };
        let password = match &config.password_secret {
            Some(n) => Some(secrets.resolve(n)?.into_bytes()),
            None => None,
        };
        let codec = JsonCodec {
            schema: config.schema.clone(),
            limits: config.json_limits,
            policy: if config.fail_on_decode {
                sparrow_formats::BadRecordPolicy::FailJob
            } else {
                sparrow_formats::BadRecordPolicy::Drop
            },
            owner: None,
        };
        Ok(Self {
            config,
            diag,
            codec,
            username,
            password,
        })
    }

    /// Pump MQTT publishes into a bounded kernel ingress. Full inbox drops
    /// (`live_best_effort`); this is not an ack.
    pub async fn run(self, tx: mpsc::Sender<Row>, cancel: CancellationToken) -> Result<()> {
        let mut backoff = self.config.reconnect_min;
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            match self.session(&tx, &cancel).await {
                Ok(()) => return Ok(()),
                Err(e)
                    if self.config.fail_on_decode
                        && matches!(
                            e.code,
                            ErrorCode::CodecViolation
                                | ErrorCode::MaxRecordSize
                                | ErrorCode::InvalidSchema
                                | ErrorCode::TypeMismatch
                                | ErrorCode::BoundExceeded
                        ) =>
                {
                    return Err(e);
                }
                Err(_) => {
                    self.diag.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(self.config.reconnect_max);
                }
            }
        }
    }

    async fn session(&self, tx: &mpsc::Sender<Row>, cancel: &CancellationToken) -> Result<()> {
        let mut stream = connect_plain(
            &self.config.host,
            self.config.port,
            &self.config.tls,
            self.config.connect_timeout,
        )
        .await?;
        let handshake = async {
            write_packet(
                &mut stream,
                &Packet::Connect(Connect {
                    client_id: self.config.client_id.clone(),
                    clean_session: self.config.clean_session,
                    keepalive: self.config.keepalive.as_secs() as u16,
                    username: self.username.clone(),
                    password: self.password.clone(),
                }),
            )
            .await?;
            let mut reader = MqttFramedReader::new();
            match reader.next(&mut stream).await? {
                Packet::ConnAck { return_code: 0, .. } => {}
                Packet::ConnAck { return_code, .. } => {
                    return Err(ConnectorError::new(
                        ErrorCode::Internal,
                        format!("MQTT CONNACK {return_code}"),
                    ));
                }
                other => {
                    return Err(ConnectorError::new(
                        ErrorCode::CodecViolation,
                        format!("expected CONNACK, got {other:?}"),
                    ));
                }
            }
            write_packet(
                &mut stream,
                &Packet::Subscribe {
                    packet_id: 1,
                    topics: vec![(self.config.topic.clone(), 0)],
                },
            )
            .await?;
            match reader.next(&mut stream).await? {
                Packet::SubAck { .. } => {}
                other => {
                    return Err(ConnectorError::new(
                        ErrorCode::CodecViolation,
                        format!("expected SUBACK, got {other:?}"),
                    ));
                }
            }
            Ok::<_, ConnectorError>(reader)
        };

        let mut reader = tokio::select! {
            _ = cancel.cancelled() => {
                close_mqtt(&mut stream).await;
                return Ok(());
            }
            r = tokio::time::timeout(self.config.connect_timeout, handshake) => {
                match r {
                    Ok(Ok(reader)) => reader,
                    Ok(Err(e)) => {
                        close_mqtt(&mut stream).await;
                        return Err(e);
                    }
                    Err(_) => {
                        close_mqtt(&mut stream).await;
                        return Err(ConnectorError::new(
                            ErrorCode::Internal,
                            "MQTT handshake timeout",
                        ));
                    }
                }
            }
        };

        let ping = self.config.keepalive / 2;
        let ping = if ping.is_zero() {
            Duration::from_secs(15)
        } else {
            ping
        };

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    close_mqtt(&mut stream).await;
                    return Ok(());
                }
                _ = tokio::time::sleep(ping) => {
                    write_packet(&mut stream, &Packet::PingReq).await?;
                }
                pkt = reader.next(&mut stream) => {
                    match pkt? {
                        Packet::Publish(Publish { payload, .. }) => {
                            self.diag.mqtt_received.fetch_add(1, Ordering::Relaxed);
                            let frame = SourceFrame::new(payload, 0);
                            match self.codec.decode_frame(&frame) {
                                Ok(Some(row)) => {
                                    self.diag.mqtt_decoded.fetch_add(1, Ordering::Relaxed);
                                    match tx.try_send(row) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            self.diag.mqtt_dropped_full.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            close_mqtt(&mut stream).await;
                                            return Ok(());
                                        }
                                    }
                                }
                                Ok(None) => {
                                    self.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                    self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    self.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                    self.diag.decode_errors.fetch_add(1, Ordering::Relaxed);
                                    if self.config.fail_on_decode {
                                        close_mqtt(&mut stream).await;
                                        return Err(ConnectorError::new(
                                            e.code,
                                            format!("MQTT decode failed (fail_on_decode): {e}"),
                                        ));
                                    }
                                }
                            }
                        }
                        Packet::PingResp => {}
                        Packet::Disconnect => {
                            close_mqtt(&mut stream).await;
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

const MQTT_STOP_DEADLINE: Duration = Duration::from_millis(400);

async fn close_mqtt(stream: &mut super::io::MqttStream) {
    let _ = tokio::time::timeout(MQTT_STOP_DEADLINE, async {
        let _ = write_packet(stream, &Packet::Disconnect).await;
        use tokio::io::AsyncWriteExt;
        let _ = stream.shutdown().await;
    })
    .await;
    // Dropping the stream closes the TCP socket even if the peer is silent.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::TargetPolicy;
    use crate::secret::MapSecretResolver;
    use sparrow_model::{DataType, Field, FieldId, SchemaId};
    use tokio::net::TcpListener;

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "device_id", DataType::Utf8, false)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn r19_stop_completes_when_broker_silent_after_accept() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((s, _)) = listener.accept().await {
                // Accept then stay silent (no CONNACK).
                let _ = tokio::time::sleep(Duration::from_secs(30)).await;
                drop(s);
            }
        });
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", port, schema());
        cfg.connect_timeout = Duration::from_secs(5);
        cfg.client_id = format!("r19-{}", std::process::id());
        let src = MqttSource::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
            IoDiagnostics::new(),
        )
        .unwrap();
        let (tx, _rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let started = std::time::Instant::now();
        let task = tokio::spawn(async move { src.run(tx, child).await });
        tokio::time::sleep(Duration::from_millis(80)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(800), task)
            .await
            .expect("MQTT stop must finish while broker is silent")
            .unwrap()
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "stop exceeded deadline: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn r19_half_frame_survives_ping_select() {
        use crate::mqtt::codec::{encode, Packet, Publish};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).await; // CONNECT
            let _ = s
                .write_all(&encode(&Packet::ConnAck {
                    session_present: false,
                    return_code: 0,
                }).unwrap())
                .await;
            let _ = s.read(&mut buf).await; // SUBSCRIBE
            let _ = s
                .write_all(&encode(&Packet::SubAck {
                    packet_id: 1,
                    codes: vec![0],
                }).unwrap())
                .await;
            let pub_bytes = encode(&Packet::Publish(Publish {
                dup: false,
                qos: 0,
                retain: false,
                topic: "sensors/json".into(),
                packet_id: None,
                payload: br#"{"device_id":"d1"}"#.to_vec(),
            }))
            .unwrap();
            s.write_all(&pub_bytes[..1]).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(180)).await;
            s.write_all(&pub_bytes[1..]).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", port, schema());
        cfg.keepalive = Duration::from_millis(80);
        cfg.connect_timeout = Duration::from_secs(2);
        cfg.client_id = format!("r19-hf-{}", std::process::id());
        let src = MqttSource::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
            IoDiagnostics::new(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        tokio::spawn(async move { src.run(tx, child).await });
        let row = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("half-frame must complete after ping interval")
            .expect("row");
        assert_eq!(row.values[0], sparrow_model::Scalar::utf8("d1"));
        cancel.cancel();
    }

    #[test]
    fn p1_27_inbox_times_max_record_rejects_over_queue_budget() {
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema());
        cfg.inbox_capacity = 40;
        cfg.json_limits.max_bytes = 64 * 1024;
        let worst = cfg.inbox_worst_case_bytes();
        assert!(worst > ResourceBudget::compact().queue_bytes);
        assert!(worst <= 4 * 1024 * 1024);
        let err = cfg
            .check_inbox_budget(ResourceBudget::compact().queue_bytes)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        assert!(
            err.message.contains("queue budget"),
            "must name the queue budget, not only 4MiB: {}",
            err.message
        );
        cfg.validate(&MapSecretResolver::empty(), &TargetPolicy::allow("127.0.0.1", 1883))
            .unwrap_err();
    }

    #[test]
    fn p1_27_inbox_within_compact_queue_is_ok() {
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema());
        cfg.inbox_capacity = 16;
        cfg.json_limits.max_bytes = 64 * 1024;
        assert!(cfg.inbox_worst_case_bytes() <= ResourceBudget::compact().queue_bytes);
        cfg.check_inbox_budget(ResourceBudget::compact().queue_bytes)
            .unwrap();
    }
}

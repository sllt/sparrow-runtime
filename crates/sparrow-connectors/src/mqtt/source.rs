use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{JsonCodec, JsonLimits};
use sparrow_model::{ErrorCode, RestoreClaim, Row, Schema, SourceFrame};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::codec::{Connect, Packet, Publish};
use super::io::{connect_plain, read_packet, write_packet};
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
        if let Some(name) = &self.username_secret {
            let _ = secrets.resolve(name)?;
        }
        if let Some(name) = &self.password_secret {
            let _ = secrets.resolve(name)?;
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
            policy: sparrow_formats::BadRecordPolicy::Drop,
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
    pub async fn run(self, tx: mpsc::Sender<Row>, cancel: CancellationToken) {
        let mut backoff = self.config.reconnect_min;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match self.session(&tx, &cancel).await {
                Ok(()) => break,
                Err(_) => {
                    self.diag.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = cancel.cancelled() => break,
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
        match read_packet(&mut stream).await? {
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
        match read_packet(&mut stream).await? {
            Packet::SubAck { .. } => {}
            other => {
                return Err(ConnectorError::new(
                    ErrorCode::CodecViolation,
                    format!("expected SUBACK, got {other:?}"),
                ));
            }
        }

        let ping = self.config.keepalive / 2;
        let ping = if ping.is_zero() {
            Duration::from_secs(15)
        } else {
            ping
        };

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = write_packet(&mut stream, &Packet::Disconnect).await;
                    return Ok(());
                }
                _ = tokio::time::sleep(ping) => {
                    write_packet(&mut stream, &Packet::PingReq).await?;
                }
                pkt = read_packet(&mut stream) => {
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
                                        Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                                    }
                                }
                                Ok(None) => {
                                    self.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    self.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Packet::PingResp => {}
                        Packet::Disconnect => return Ok(()),
                        _ => {}
                    }
                }
            }
        }
    }
}

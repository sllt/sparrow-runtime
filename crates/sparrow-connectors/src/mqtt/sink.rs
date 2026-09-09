//! MQTT QoS0 sink. Publishes JSON rows. Replay unsupported.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{encode_json_row, JsonLimits};
use sparrow_model::{ErrorCode, RestoreClaim, RowBatch};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::codec::{Connect, Packet, Publish};
use super::io::{connect_plain, read_packet, write_packet, MqttFramedReader};
use crate::capabilities::{
    refuse_dirty_session, refuse_durable_recovery, refuse_qos_durable, ConnectorCapabilities,
};
use crate::diag::IoDiagnostics;
use crate::error::{ConnectorError, Result};
use crate::policy::TargetPolicy;
use crate::secret::SecretResolver;
use crate::tls::TlsConfig;

const MAX_OUTBOX: usize = 1024;

#[derive(Clone, Debug)]
pub struct MqttSinkConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub qos: u8,
    pub clean_session: bool,
    pub tls: TlsConfig,
    pub connect_timeout: Duration,
    pub keepalive: Duration,
    pub outbox_capacity: usize,
    pub restore: RestoreClaim,
}

impl MqttSinkConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::MQTT_SINK
    }

    pub fn demo(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            client_id: "sparrow-mqtt-sink".into(),
            topic: "sparrow/out".into(),
            qos: 0,
            clean_session: true,
            tls: TlsConfig::disabled(),
            connect_timeout: Duration::from_secs(2),
            keepalive: Duration::from_secs(30),
            outbox_capacity: 32,
            restore: RestoreClaim::None,
        }
    }

    pub fn validate(&self, _secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_qos_durable(self.qos)?;
        refuse_dirty_session(self.clean_session)?;
        refuse_durable_recovery(&self.restore)?;
        self.tls.validate()?;
        if self.host.is_empty() || self.client_id.is_empty() || self.topic.is_empty() {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "MQTT sink host, client_id, and topic are required",
            ));
        }
        if self.outbox_capacity == 0 || self.outbox_capacity > MAX_OUTBOX {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "MQTT sink outbox_capacity {} is outside 1..={MAX_OUTBOX}",
                    self.outbox_capacity
                ),
            ));
        }
        policy.check_host_port(&self.host, self.port)?;
        Ok(())
    }
}

pub struct MqttSink {
    pub config: MqttSinkConfig,
    pub diag: Arc<IoDiagnostics>,
}

impl MqttSink {
    pub fn bind(
        config: MqttSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        Ok(Self { config, diag })
    }

    pub async fn run(self, mut rx: mpsc::Receiver<RowBatch>, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match self.session(&mut rx, &cancel).await {
                Ok(()) => break,
                Err(_) => {
                    self.diag.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
            }
        }
    }

    async fn session(
        &self,
        rx: &mut mpsc::Receiver<RowBatch>,
        cancel: &CancellationToken,
    ) -> Result<()> {
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
                    format!("MQTT sink CONNACK {other:?}"),
                ));
            }
        }
        let limits = JsonLimits::default();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = write_packet(&mut stream, &Packet::Disconnect).await;
                    return Ok(());
                }
                next = rx.recv() => {
                    match next {
                        Some(batch) => {
                            let schema = batch.schema();
                            for row in batch.rows() {
                                let body = match encode_json_row(schema, row) {
                                    Ok(b) if b.len() <= limits.max_bytes => b,
                                    _ => {
                                        self.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                };
                                write_packet(
                                    &mut stream,
                                    &Packet::Publish(Publish {
                                        dup: false,
                                        qos: 0,
                                        retain: false,
                                        topic: self.config.topic.clone(),
                                        packet_id: None,
                                        payload: body,
                                    }),
                                )
                                .await?;
                                self.diag.mqtt_decoded.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        None => {
                            let _ = write_packet(&mut stream, &Packet::Disconnect).await;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

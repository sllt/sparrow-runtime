use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{JsonCodec, JsonLimits};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryOwner, QueuedRow, ResourceBudget, RestoreClaim, Row, Schema,
    SourceFrame,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::codec::{Connect, Packet, Publish};
use super::io::{connect_with_quickack, write_packet, MqttFramedReader};
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
    /// Maximum wait for a full inbox. Zero preserves immediate best-effort drop.
    /// The pump holds only the current row and keeps servicing keepalive/stop.
    pub inbox_wait_timeout: Duration,
    pub tcp_quickack: bool,
    /// Some enables byte-accounted ingress (must use run_budgeted).
    pub inbox_bytes: Option<usize>,
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

    pub fn validate(&self, secrets: &dyn SecretResolver, policy: &TargetPolicy) -> Result<()> {
        refuse_qos_durable(self.qos)?;
        #[cfg(not(target_os = "linux"))]
        if self.tcp_quickack {
            return Err(ConnectorError::new(
                ErrorCode::FeatureUnavailable,
                "TCP_QUICKACK requires Linux",
            ));
        }
        refuse_dirty_session(self.clean_session)?;
        refuse_durable_recovery(&self.restore)?;
        self.tls.validate()?;
        if self.host.is_empty() || self.client_id.is_empty() || self.topic.is_empty() {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "MQTT host, client_id, and topic are required",
            ));
        }
        let max_inbox = if self.inbox_bytes.is_some() {
            4096
        } else {
            MAX_INBOX
        };
        if self.inbox_capacity == 0 || self.inbox_capacity > max_inbox {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                format!(
                    "MQTT inbox_capacity {} is outside 1..={max_inbox} (buffers are bounded)",
                    self.inbox_capacity
                ),
            ));
        }
        if self.inbox_wait_timeout > Duration::from_secs(1) {
            return Err(ConnectorError::new(
                ErrorCode::BoundExceeded,
                "MQTT inbox_wait_ms must be in 0..=1000",
            ));
        }
        policy.check_host_port(&self.host, self.port)?;
        if (self.username_secret.is_some() || self.password_secret.is_some()) && !self.tls.enabled {
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
        if let Some(bytes) = self.inbox_bytes {
            if bytes == 0
                || bytes.saturating_add(QueuedRow::channel_budget(self.inbox_capacity)) > queue_budget.min(4 * 1024 * 1024)
                || self.json_limits.max_bytes > 64 * 1024
            {
                return Err(ConnectorError::new(
                    ErrorCode::BoundExceeded,
                    "MQTT inbox_bytes plus channel metadata must fit queue budget/4MiB; wire records remain <=64KiB",
                ));
            }
            return Ok(());
        }
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
            inbox_wait_timeout: Duration::from_millis(5),
            tcp_quickack: false,
            inbox_bytes: None,
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

enum Ingress {
    Plain(mpsc::Sender<Row>),
    Budgeted {
        tx: mpsc::Sender<QueuedRow>,
        owner: Arc<MemoryOwner>,
        queue: Arc<MemoryOwner>,
        max_row_bytes: usize,
    },
}

struct PendingIngress {
    diag: Arc<IoDiagnostics>,
    bytes: u64,
}
impl Drop for PendingIngress {
    fn drop(&mut self) {
        self.diag
            .mqtt_pending_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
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

    /// Pump MQTT publishes into a bounded kernel ingress. A full inbox gets a
    /// bounded wait before dropping (`live_best_effort`); this is not an ack.
    pub async fn run(self, tx: mpsc::Sender<Row>, cancel: CancellationToken) -> Result<()> {
        if self.config.inbox_bytes.is_some() {
            return Err(ConnectorError::new(
                ErrorCode::InvalidArgument,
                "inbox_bytes requires run_budgeted",
            ));
        }
        self.pump(Ingress::Plain(tx), cancel).await
    }

    pub async fn run_budgeted(
        self,
        tx: mpsc::Sender<QueuedRow>,
        cancel: CancellationToken,
        owner: Arc<MemoryOwner>,
        max_row_bytes: usize,
    ) -> Result<()> {
        if tx.max_capacity() != self.config.inbox_capacity {
            return Err(ConnectorError::new(ErrorCode::InvalidArgument, "MQTT inbox_capacity does not match channel"));
        }
        let bytes = self.config.inbox_bytes.ok_or_else(|| {
            ConnectorError::new(ErrorCode::InvalidArgument, "inbox_bytes required")
        })?;
        self.config.check_inbox_budget(owner.budget().queue_bytes)?;
        let mut budget = owner.budget();
        budget.queue_bytes = bytes;
        let queue = MemoryOwner::child(owner.clone(), budget, "mqtt-inbox");
        self.diag.mqtt_accounted_sources.store(1, Ordering::Relaxed);
        self.diag.mqtt_inbox_metadata_bytes.store(QueuedRow::channel_budget(self.config.inbox_capacity) as u64, Ordering::Relaxed);
        self.pump(
            Ingress::Budgeted {
                tx,
                owner,
                queue,
                max_row_bytes,
            },
            cancel,
        )
        .await
    }

    async fn pump(self, tx: Ingress, cancel: CancellationToken) -> Result<()> {
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

    async fn session(&self, tx: &Ingress, cancel: &CancellationToken) -> Result<()> {
        let mut stream = connect_with_quickack(
            &self.config.host,
            self.config.port,
            &self.config.tls,
            self.config.connect_timeout,
            self.config.tcp_quickack,
            Some(self.diag.clone()),
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
        // Keepalive concerns packets sent by this client. Inbound PUBLISH (or
        // PINGRESP) must not postpone PINGREQ, including when the inbox is full.
        let mut next_ping = tokio::time::Instant::now() + ping;

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    close_mqtt(&mut stream).await;
                    return Ok(());
                }
                _ = tokio::time::sleep_until(next_ping) => {
                    write_packet(&mut stream, &Packet::PingReq).await?;
                    next_ping = tokio::time::Instant::now() + ping;
                }
                pkt = reader.next(&mut stream) => {
                    match pkt? {
                        Packet::Publish(Publish { payload, .. }) => {
                            self.diag.mqtt_received.fetch_add(1, Ordering::Relaxed);
                            let frame = SourceFrame::new(payload, 0);
                            let decoded = self.codec.decode_frame(&frame);
                            drop(frame);
                            match decoded {
                                Ok(Some(row)) => {
                                    self.diag.mqtt_decoded.fetch_add(1, Ordering::Relaxed);
                                    if let Ingress::Budgeted { tx, owner, queue, max_row_bytes } = tx {
                                        if !self.deliver_budgeted(row, tx, owner, queue, *max_row_bytes, &mut stream, cancel, &mut next_ping, ping).await? {
                                            close_mqtt(&mut stream).await;
                                            return Ok(());
                                        }
                                        continue;
                                    }
                                    let Ingress::Plain(tx) = tx else { unreachable!() };
                                    match tx.try_send(row) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(row)) => {
                                            if self.config.inbox_wait_timeout.is_zero() {
                                                self.diag.mqtt_dropped_full.fetch_add(1, Ordering::Relaxed);
                                            } else if !self.wait_for_inbox(
                                                row, tx, &mut stream, cancel, &mut next_ping, ping,
                                            ).await? {
                                                close_mqtt(&mut stream).await;
                                                return Ok(());
                                            }
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

    async fn deliver_budgeted(
        &self,
        row: Row,
        tx: &mpsc::Sender<QueuedRow>,
        owner: &Arc<MemoryOwner>,
        queue: &Arc<MemoryOwner>,
        max_row_bytes: usize,
        stream: &mut super::io::MqttStream,
        cancel: &CancellationToken,
        next_ping: &mut tokio::time::Instant,
        ping: Duration,
    ) -> Result<bool> {
        let bytes = QueuedRow::accounted_bytes(&row);
        if bytes > queue.budget().queue_bytes.min(max_row_bytes) {
            self.diag
                .mqtt_dropped_oversize
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        // The single decoded working row is Reservation-billed while waiting
        // for Queue credit. Admission and handoff overlap charges, never leave
        // a live queued row unbilled. Decode itself is the bounded codec scratch.
        let Ok(_working) = owner.acquire(CreditKind::Reservation, bytes) else {
            self.diag
                .mqtt_dropped_budget
                .fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        };
        self.diag
            .mqtt_pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        let _pending = PendingIngress {
            diag: self.diag.clone(),
            bytes: bytes as u64,
        };
        let deadline = tokio::time::Instant::now() + self.config.inbox_wait_timeout;
        let mut row = Some(row);
        let mut waited = false;
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let budget_full = match tx.try_reserve() {
                Ok(permit) => {
                    match QueuedRow::try_new(row.take().unwrap(), queue, &self.diag.mqtt_inbox) {
                        Ok(row) => {
                            permit.send(row);
                            if waited {
                                self.diag
                                    .mqtt_backpressure_recovered
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            return Ok(true);
                        }
                        Err((r, _)) => {
                            row = Some(r);
                            true
                        }
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => false,
            };
            if tokio::time::Instant::now() >= deadline {
                self.diag.mqtt_dropped_full.fetch_add(1, Ordering::Relaxed);
                if budget_full {
                    self.diag
                        .mqtt_dropped_budget
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(true);
            }
            if !waited {
                waited = true;
                self.diag
                    .mqtt_backpressure_waits
                    .fetch_add(1, Ordering::Relaxed);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(false),
                _ = tokio::time::sleep_until(deadline) => {
                    self.diag.mqtt_dropped_full.fetch_add(1, Ordering::Relaxed);
                    if budget_full { self.diag.mqtt_dropped_budget.fetch_add(1, Ordering::Relaxed); }
                    return Ok(true);
                }
                _ = tokio::time::sleep_until(*next_ping) => {
                    write_packet(stream, &Packet::PingReq).await?;
                    *next_ping = tokio::time::Instant::now() + ping;
                }
                _ = async {
                    if budget_full {
                        // Credit may be freed by any sibling stage/job. A 1 ms
                        // bounded retry avoids a new global waiter registry.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else { let _ = tx.reserve().await; }
                } => {},
            }
        }
    }

    /// No read-ahead while blocked: at most the already-decoded row is held
    /// outside the bounded channel. Reserve before moving it, so a heartbeat
    /// cannot cancel a send and silently lose the row. Both the reservation and
    /// the absolute timeout survive pings; FIFO is preserved by the single pump.
    async fn wait_for_inbox(
        &self,
        row: Row,
        tx: &mpsc::Sender<Row>,
        stream: &mut super::io::MqttStream,
        cancel: &CancellationToken,
        next_ping: &mut tokio::time::Instant,
        ping: Duration,
    ) -> Result<bool> {
        self.diag
            .mqtt_backpressure_waits
            .fetch_add(1, Ordering::Relaxed);
        let deadline = tokio::time::sleep(self.config.inbox_wait_timeout);
        let permit = tx.reserve();
        tokio::pin!(deadline, permit);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(false),
                _ = &mut deadline => {
                    self.diag.mqtt_dropped_full.fetch_add(1, Ordering::Relaxed);
                    return Ok(true);
                }
                _ = tokio::time::sleep_until(*next_ping) => {
                    write_packet(stream, &Packet::PingReq).await?;
                    *next_ping = tokio::time::Instant::now() + ping;
                }
                reserved = &mut permit => {
                    let Ok(permit) = reserved else { return Ok(false) };
                    permit.send(row);
                    self.diag.mqtt_backpressure_recovered.fetch_add(1, Ordering::Relaxed);
                    return Ok(true);
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
            vec![Field::new(
                FieldId::new(1),
                "device_id",
                DataType::Utf8,
                false,
            )],
        )
        .unwrap()
    }

    fn inbox_source(wait: Duration) -> MqttSource {
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema());
        cfg.inbox_wait_timeout = wait;
        MqttSource::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", 1883),
            IoDiagnostics::new(),
        )
        .unwrap()
    }

    fn row(id: &str) -> Row {
        Row {
            values: vec![sparrow_model::Scalar::utf8(id)],
        }
    }

    #[tokio::test(start_paused = true)]
    async fn byte_budget_wait_times_out_or_recovers_and_releases_working_credit() {
        for recover in [false, true] {
            let source = inbox_source(Duration::from_millis(5));
            let owner = MemoryOwner::new(ResourceBudget::compact());
            let mut budget = owner.budget();
            budget.queue_bytes = QueuedRow::accounted_bytes(&row("same"));
            let queue = MemoryOwner::child(owner.clone(), budget, "inbox-test");
            let (tx, mut rx) = mpsc::channel(4);
            tx.send(QueuedRow::try_new(row("same"), &queue, &source.diag.mqtt_inbox).unwrap())
                .await
                .unwrap();
            let (socket, _peer) = tokio::io::duplex(64);
            let mut stream: super::super::io::MqttStream = Box::pin(socket);
            let cancel = CancellationToken::new();
            let ping = Duration::from_secs(1);
            let mut next_ping = tokio::time::Instant::now() + ping;
            let send = source.deliver_budgeted(
                row("same"),
                &tx,
                &owner,
                &queue,
                4096,
                &mut stream,
                &cancel,
                &mut next_ping,
                ping,
            );
            let receive = async {
                if recover {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    drop(rx.recv().await.unwrap());
                }
            };
            let (result, _) = tokio::join!(send, receive);
            assert!(result.unwrap());
            let diag = source.diag.snapshot();
            assert_eq!(diag.mqtt_backpressure_waits, 1);
            assert_eq!(diag.mqtt_backpressure_recovered, u64::from(recover));
            assert_eq!(diag.mqtt_dropped_budget, u64::from(!recover));
            assert_eq!(diag.mqtt_pending_bytes, 0);
            assert_eq!(owner.usage().reservation_bytes, 0);
            drop(rx);
            assert_eq!(owner.usage().physical_bytes, 0);
            assert_eq!(source.diag.mqtt_inbox.items.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn byte_budget_rejects_oversized_row_without_poisoning_next_row() {
        let source = inbox_source(Duration::from_millis(5));
        let owner = MemoryOwner::new(ResourceBudget::compact());
        let (tx, mut rx) = mpsc::channel(4);
        let (socket, _peer) = tokio::io::duplex(64);
        let mut stream: super::super::io::MqttStream = Box::pin(socket);
        let cancel = CancellationToken::new();
        let ping = Duration::from_secs(1);
        let mut next_ping = tokio::time::Instant::now() + ping;
        for r in [row(&"x".repeat(8192)), row("ok")] {
            assert!(source
                .deliver_budgeted(
                    r,
                    &tx,
                    &owner,
                    &owner,
                    4096,
                    &mut stream,
                    &cancel,
                    &mut next_ping,
                    ping
                )
                .await
                .unwrap());
        }
        assert_eq!(source.diag.snapshot().mqtt_dropped_oversize, 1);
        drop(rx.recv().await.unwrap());
        assert!(rx.try_recv().is_err());
        assert_eq!(owner.usage().physical_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure_recovers_without_reordering_or_duplicating() {
        let source = inbox_source(Duration::from_millis(5));
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(row("first")).unwrap();
        let (socket, _peer) = tokio::io::duplex(64);
        let mut stream: super::super::io::MqttStream = Box::pin(socket);
        let cancel = CancellationToken::new();
        let ping = Duration::from_secs(1);
        let mut next_ping = tokio::time::Instant::now() + ping;
        let waiting = source.wait_for_inbox(
            row("second"),
            &tx,
            &mut stream,
            &cancel,
            &mut next_ping,
            ping,
        );
        tokio::pin!(waiting);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(source.diag.snapshot().mqtt_backpressure_waits, 1);
        assert_eq!(rx.recv().await.unwrap(), row("first"));
        assert!(waiting.await.unwrap());
        assert_eq!(rx.recv().await.unwrap(), row("second"));
        assert!(rx.try_recv().is_err());
        let diag = source.diag.snapshot();
        assert_eq!(diag.mqtt_backpressure_recovered, 1);
        assert_eq!(diag.mqtt_dropped_full, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure_timeout_is_not_reset_by_heartbeats() {
        use tokio::io::AsyncReadExt;
        let source = inbox_source(Duration::from_millis(50));
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(row("first")).unwrap();
        let (socket, mut peer) = tokio::io::duplex(64);
        let mut stream: super::super::io::MqttStream = Box::pin(socket);
        let cancel = CancellationToken::new();
        let ping = Duration::from_millis(10);
        let start = tokio::time::Instant::now();
        let mut next_ping = start + ping;
        assert!(tokio::time::timeout(
            Duration::from_millis(100),
            source.wait_for_inbox(row("drop"), &tx, &mut stream, &cancel, &mut next_ping, ping,)
        )
        .await
        .unwrap()
        .unwrap());
        assert!(start.elapsed() >= Duration::from_millis(50));
        assert!(start.elapsed() < Duration::from_millis(60));
        let mut pings = [0; 8];
        peer.read_exact(&mut pings).await.unwrap();
        assert_eq!(pings, [0xc0, 0, 0xc0, 0, 0xc0, 0, 0xc0, 0]);
        assert_eq!(rx.recv().await.unwrap(), row("first"));
        assert!(rx.try_recv().is_err());
        let diag = source.diag.snapshot();
        assert_eq!(diag.mqtt_backpressure_waits, 1);
        assert_eq!(diag.mqtt_backpressure_recovered, 0);
        assert_eq!(diag.mqtt_dropped_full, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure_stop_and_closed_receiver_interrupt_wait() {
        for close_receiver in [false, true] {
            let source = inbox_source(Duration::from_secs(1));
            let (tx, mut rx) = mpsc::channel(1);
            tx.try_send(row("first")).unwrap();
            let (socket, _peer) = tokio::io::duplex(64);
            let mut stream: super::super::io::MqttStream = Box::pin(socket);
            let cancel = CancellationToken::new();
            let ping = Duration::from_secs(1);
            let start = tokio::time::Instant::now();
            let mut next_ping = start + ping;
            let waiting = source.wait_for_inbox(
                row("abandoned"),
                &tx,
                &mut stream,
                &cancel,
                &mut next_ping,
                ping,
            );
            tokio::pin!(waiting);
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            if close_receiver {
                rx.close();
            } else {
                cancel.cancel();
            }
            assert!(!waiting.await.unwrap());
            assert_eq!(start.elapsed(), Duration::ZERO);
            assert_eq!(source.diag.snapshot().mqtt_dropped_full, 0);
            assert_eq!(source.diag.snapshot().mqtt_backpressure_recovered, 0);
        }
    }

    #[test]
    fn inbox_wait_defaults_and_bounds() {
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", 1883, schema());
        assert_eq!(cfg.inbox_wait_timeout, Duration::from_millis(5));
        for ms in [0, 5, 1000, 1001] {
            cfg.inbox_wait_timeout = Duration::from_millis(ms);
            let result = cfg.validate(
                &MapSecretResolver::empty(),
                &TargetPolicy::allow("127.0.0.1", 1883),
            );
            if ms <= 1000 {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().code, ErrorCode::BoundExceeded);
            }
        }
    }

    async fn burst_broker(count: usize) -> (u16, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            socket.set_nodelay(true).unwrap();
            let mut stream: super::super::io::MqttStream = Box::pin(socket);
            let mut reader = MqttFramedReader::new();
            assert!(matches!(
                reader.next(&mut stream).await.unwrap(),
                Packet::Connect(_)
            ));
            write_packet(
                &mut stream,
                &Packet::ConnAck {
                    session_present: false,
                    return_code: 0,
                },
            )
            .await
            .unwrap();
            let Packet::Subscribe { packet_id, .. } = reader.next(&mut stream).await.unwrap()
            else {
                panic!("SUBSCRIBE")
            };
            write_packet(
                &mut stream,
                &Packet::SubAck {
                    packet_id,
                    codes: vec![0],
                },
            )
            .await
            .unwrap();
            let mut bytes = Vec::new();
            for n in 0..count {
                bytes.extend(
                    super::super::codec::encode(&Packet::Publish(Publish {
                        dup: false,
                        qos: 0,
                        retain: false,
                        topic: "sensors/json".into(),
                        packet_id: None,
                        payload: format!(r#"{{"device_id":"{n}"}}"#).into_bytes(),
                    }))
                    .unwrap(),
                );
            }
            stream.write_all(&bytes).await.unwrap();
            loop {
                match reader.next(&mut stream).await.unwrap() {
                    Packet::PingReq => write_packet(&mut stream, &Packet::PingResp).await.unwrap(),
                    Packet::Disconnect => return,
                    packet => panic!("unexpected {packet:?}"),
                }
            }
        });
        (port, task)
    }

    #[tokio::test]
    async fn backpressure_burst_pump_preserves_all_rows() {
        let (port, broker) = burst_broker(512).await;
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", port, schema());
        // A long test-only grace avoids making this correctness test depend on
        // host scheduling jitter. The default 5 ms is exercised by rate sweeps.
        cfg.inbox_wait_timeout = Duration::from_secs(1);
        let diag = IoDiagnostics::new();
        let source = MqttSource::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
            diag.clone(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(source.run(tx, cancel.clone()));
        tokio::time::timeout(Duration::from_secs(3), async {
            while diag.mqtt_backpressure_waits.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
            for n in 0..512 {
                assert_eq!(rx.recv().await.unwrap(), row(&n.to_string()));
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), broker)
            .await
            .unwrap()
            .unwrap();
        let diag = diag.snapshot();
        assert_eq!(diag.mqtt_received, 512);
        assert_eq!(diag.mqtt_decoded, 512);
        assert_eq!(diag.mqtt_dropped_full, 0);
        assert_eq!(diag.mqtt_reconnects, 0);
        assert_eq!(
            diag.mqtt_backpressure_waits,
            diag.mqtt_backpressure_recovered
        );
    }

    #[tokio::test]
    async fn backpressure_zero_wait_preserves_immediate_drop() {
        let (port, broker) = burst_broker(2).await;
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", port, schema());
        cfg.inbox_wait_timeout = Duration::ZERO;
        let diag = IoDiagnostics::new();
        let source = MqttSource::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
            diag.clone(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(source.run(tx, cancel.clone()));
        tokio::time::timeout(Duration::from_secs(3), async {
            while diag.mqtt_dropped_full.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(rx.recv().await.unwrap(), row("0"));
        assert!(rx.try_recv().is_err());
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), broker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(diag.snapshot().mqtt_dropped_full, 1);
        assert_eq!(diag.snapshot().mqtt_backpressure_waits, 0);
    }

    #[tokio::test]
    async fn keepalive_pings_during_continuous_inbound_traffic() {
        assert_keepalive_progress(true).await;
    }

    #[tokio::test]
    async fn keepalive_pings_while_idle() {
        assert_keepalive_progress(false).await;
    }

    async fn assert_keepalive_progress(publish: bool) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (alive_tx, alive_rx) = tokio::sync::oneshot::channel();
        let broker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            socket.set_nodelay(true).unwrap();
            let mut stream: super::super::io::MqttStream = Box::pin(socket);
            let mut reader = MqttFramedReader::new();
            let Packet::Connect(connect) = reader.next(&mut stream).await.unwrap() else {
                panic!("expected CONNECT");
            };
            assert_eq!(connect.keepalive, 1);
            write_packet(
                &mut stream,
                &Packet::ConnAck {
                    session_present: false,
                    return_code: 0,
                },
            )
            .await
            .unwrap();
            let Packet::Subscribe { packet_id, .. } = reader.next(&mut stream).await.unwrap()
            else {
                panic!("expected SUBSCRIBE");
            };
            write_packet(
                &mut stream,
                &Packet::SubAck {
                    packet_id,
                    codes: vec![0],
                },
            )
            .await
            .unwrap();
            let mut incoming = tokio::time::interval(Duration::from_millis(20));
            let mut deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
            let mut pings = 0;
            let mut alive_tx = Some(alive_tx);
            loop {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => {
                        panic!("keepalive expired despite continuous broker-to-client traffic");
                    }
                    packet = reader.next(&mut stream) => {
                        match packet.unwrap() {
                            Packet::PingReq => {
                                pings += 1;
                                deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
                                write_packet(&mut stream, &Packet::PingResp).await.unwrap();
                                if pings == 3 {
                                    alive_tx.take().unwrap().send(()).unwrap();
                                }
                            }
                            Packet::Disconnect => return pings,
                            other => panic!("unexpected subscriber packet: {other:?}"),
                        }
                    }
                    _ = incoming.tick(), if publish => {
                        write_packet(&mut stream, &Packet::Publish(Publish {
                            dup: false, qos: 0, retain: false,
                            topic: "sensors/json".into(), packet_id: None,
                            payload: br#"{"device_id":"alive"}"#.to_vec(),
                        })).await.unwrap();
                    }
                }
            }
        });
        let mut cfg = MqttSourceConfig::demo("127.0.0.1", port, schema());
        cfg.keepalive = Duration::from_secs(1);
        let diag = IoDiagnostics::new();
        let source = MqttSource::bind(
            cfg,
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", port),
            Arc::clone(&diag),
        )
        .unwrap();
        // A full inbox must not starve keepalive either; the best-effort drops
        // remain counted, while the same TCP session survives multiple periods.
        let (tx, _rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let task = tokio::spawn(async move { source.run(tx, child).await });
        let alive = tokio::time::timeout(Duration::from_secs(4), alive_rx).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let pings = tokio::time::timeout(Duration::from_secs(2), broker)
            .await
            .unwrap()
            .unwrap();
        alive
            .expect("keepalive deadline")
            .expect("broker expired without receiving PINGREQ");
        assert!(pings >= 3);
        assert_eq!(diag.mqtt_reconnects.load(Ordering::Relaxed), 0);
        if publish {
            assert!(diag.mqtt_received.load(Ordering::Relaxed) > 3);
            assert!(diag.mqtt_dropped_full.load(Ordering::Relaxed) > 0);
        }
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
                .write_all(
                    &encode(&Packet::ConnAck {
                        session_present: false,
                        return_code: 0,
                    })
                    .unwrap(),
                )
                .await;
            let _ = s.read(&mut buf).await; // SUBSCRIBE
            let _ = s
                .write_all(
                    &encode(&Packet::SubAck {
                        packet_id: 1,
                        codes: vec![0],
                    })
                    .unwrap(),
                )
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
        cfg.validate(
            &MapSecretResolver::empty(),
            &TargetPolicy::allow("127.0.0.1", 1883),
        )
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

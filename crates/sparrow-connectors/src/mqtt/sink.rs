//! MQTT QoS0 sink. Publishes JSON rows. Replay unsupported.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::{encode_json_row, JsonLimits};
use sparrow_model::{ErrorCode, InflightCounter, RestoreClaim, RowBatch};
use tokio_util::sync::CancellationToken;

use super::codec::{Connect, Packet, Publish};
use super::io::{connect_plain, read_packet, write_packet, MqttFramedReader};
use crate::capabilities::{
    refuse_dirty_session, refuse_durable_recovery, refuse_qos_durable, ConnectorCapabilities,
};
use crate::diag::IoDiagnostics;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::{HealthState,Latency};
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
struct BatchReceipt(Option<Arc<InflightCounter>>);
impl BatchReceipt {fn ack(&mut self){if let Some(outbox)=self.0.take(){outbox.ack();}}}
impl Drop for BatchReceipt{fn drop(&mut self){if let Some(outbox)=self.0.take(){outbox.fail();}}}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use sparrow_model::{MemoryOwner,ResourceBudget};
    use crate::secret::MapSecretResolver;
    #[tokio::test]
    async fn obs_mqtt_sink_idle_ping_and_disconnect_are_observed() {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let port=listener.local_addr().unwrap().port();
        let (done_tx,done_rx)=tokio::sync::oneshot::channel();
        let broker=tokio::spawn(async move{
            let (socket,_)=listener.accept().await.unwrap();let mut stream:super::super::io::MqttStream=Box::pin(socket);
            let mut reader=MqttFramedReader::new();assert!(matches!(reader.next(&mut stream).await.unwrap(),Packet::Connect(_)));
            write_packet(&mut stream,&Packet::ConnAck{session_present:false,return_code:0}).await.unwrap();
            for _ in 0..2{assert!(matches!(reader.next(&mut stream).await.unwrap(),Packet::PingReq));write_packet(&mut stream,&Packet::PingResp).await.unwrap();}
            done_tx.send(()).unwrap();
        });
        let owner=MemoryOwner::new(ResourceBudget::compact());let diag=IoDiagnostics::new();diag.observation.initialize(&owner).unwrap();
        let mut cfg=MqttSinkConfig::demo("127.0.0.1",port);cfg.keepalive=Duration::from_millis(40);cfg.connect_timeout=Duration::from_millis(200);
        let sink=MqttSink::bind(cfg,&MapSecretResolver::empty(),&TargetPolicy::allow("127.0.0.1",port),diag.clone()).unwrap();
        let (_tx,rx)=tokio::sync::mpsc::channel(2);let cancel=CancellationToken::new();let task=tokio::spawn(sink.run(rx,cancel.clone(),None));
        tokio::time::timeout(Duration::from_secs(2),done_rx).await.unwrap().unwrap();broker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2),async{loop{if diag.observation.endpoints().unwrap().1.failures>0{break;}tokio::task::yield_now().await;}}).await.unwrap();
        cancel.cancel();tokio::time::timeout(Duration::from_secs(2),task).await.unwrap().unwrap();
        assert_eq!(diag.observation.endpoints().unwrap().1.state,HealthState::Stopped);
    }
    #[test]
    fn obs_mqtt_partial_or_cancelled_batch_fails_receipt(){
        let counter=Arc::new(InflightCounter::new());counter.enqueue();counter.enqueue();
        {let mut success=BatchReceipt(Some(counter.clone()));success.ack();let _failed=BatchReceipt(Some(counter.clone()));}
        assert_eq!((counter.pending(),counter.acked(),counter.failed()),(0,1,1));
    }
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

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx=rx.into();
        let _lifecycle=self.diag.observation.lifecycle(false);
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match self.session(&mut rx, &cancel, outbox.as_ref()).await {
                Ok(()) => break,
                Err(error) => {
                    self.diag.observation.health(false,HealthState::Reconnecting,"mqtt_sink_session_failed",Some(error.code));
                    self.diag.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
            }
        }
        rx.close();
        while let Ok(_batch)=rx.discard_next(){if let Some(outbox)=&outbox{outbox.fail();}}
    }

    async fn session(
        &self,
        rx: &mut ObservedReceiver<RowBatch>,
        cancel: &CancellationToken,
        outbox: Option<&Arc<InflightCounter>>,
    ) -> Result<()> {
        self.diag.observation.health(false,HealthState::Connecting,"mqtt_sink_handshake",None);
        let mut stream = connect_plain(
            &self.config.host,
            self.config.port,
            &self.config.tls,
            self.config.connect_timeout,
        )
        .await?;
        let handshake=async {
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
        Ok::<_,ConnectorError>(reader)
        };
        let mut reader=tokio::select!{
            _=cancel.cancelled()=>return Ok(()),
            result=tokio::time::timeout(self.config.connect_timeout,handshake)=>result.map_err(|_|ConnectorError::new(ErrorCode::Internal,"MQTT sink handshake timeout"))??,
        };
        let half=self.config.keepalive/2;
        let ping=if half.is_zero(){Duration::from_secs(15)}else{half};
        let mut next_ping=tokio::time::Instant::now()+ping;
        let mut ping_deadline=None;
        let limits = JsonLimits::default();
        self.diag.observation.health(false,HealthState::Ready,"mqtt_sink_connected_qos0",None);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    super::source::close_mqtt(&mut stream).await;
                    return Ok(());
                }
                _=async{match ping_deadline{Some(at)=>tokio::time::sleep_until(at).await,None=>std::future::pending().await}}=>{
                    return Err(ConnectorError::new(ErrorCode::Internal,"MQTT sink PINGRESP timeout"));
                }
                _=tokio::time::sleep_until(next_ping)=>{
                    tokio::time::timeout(self.config.connect_timeout,write_packet(&mut stream,&Packet::PingReq)).await
                        .map_err(|_|ConnectorError::new(ErrorCode::Internal,"MQTT sink ping timeout"))??;
                    next_ping=tokio::time::Instant::now()+ping;
                    if ping_deadline.is_none(){ping_deadline=Some(tokio::time::Instant::now()+ping*2);}
                }
                packet=reader.next(&mut stream)=>{
                    match packet?{Packet::Disconnect=>return Err(ConnectorError::new(ErrorCode::Internal,"MQTT sink broker disconnected")),Packet::PingResp=>ping_deadline=None,_=>{}}
                }
                next = rx.recv() => {
                    match next {
                        Some(batch) => {
                            let mut receipt=BatchReceipt(outbox.cloned());
                            let mut delivered=self.diag.observation.delivery_guard(batch.num_rows(),batch.tracked_bytes(),batch.origin());
                            let mut all_encoded=true;
                            let schema = batch.schema();
                            for row in batch.rows() {
                                let started=std::time::Instant::now();
                                let encoded=encode_json_row(schema,row);
                                self.diag.observation.record(Latency::Encode,started.elapsed());
                                let body = match encoded {
                                    Ok(b) if b.len() <= limits.max_bytes => b,
                                    _ => {
                                        all_encoded=false;
                                        self.diag.observation.health(false,HealthState::Failed,"mqtt_sink_encode_failed",Some(ErrorCode::CodecViolation));
                                        self.diag.mqtt_dropped_bad.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                };
                                let packet=Packet::Publish(Publish {
                                    dup:false,qos:0,retain:false,topic:self.config.topic.clone(),packet_id:None,payload:body,
                                });
                                tokio::select!{
                                    _=cancel.cancelled()=>return Ok(()),
                                    result=tokio::time::timeout(self.config.connect_timeout,write_packet(
                                    &mut stream,
                                    &packet,
                                ))=>result.map_err(|_|ConnectorError::new(ErrorCode::Internal,"MQTT sink write timeout"))??,
                                }
                                next_ping=tokio::time::Instant::now()+ping;
                                self.diag.mqtt_decoded.fetch_add(1, Ordering::Relaxed);
                                self.diag.observation.progress(false,1);
                            }
                            if all_encoded {delivered.complete();receipt.ack();}
                        }
                        None => {
                            super::source::close_mqtt(&mut stream).await;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

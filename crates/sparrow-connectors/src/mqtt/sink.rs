//! MQTT QoS0 sink. Publishes JSON rows. Replay unsupported.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use sparrow_formats::JsonLimits;
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
    /// Message payload format (JSON default, or one CSV record).
    pub payload_format: sparrow_formats::PayloadFormat,
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
            payload_format: Default::default(),
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
    action: Option<Box<sparrow_formats::action::ActionSpec>>,
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
    async fn actions_mqtt_wire_topic_payload_and_destination_rejection() {
        use sparrow_model::{Schema,SchemaId,Field,FieldId,DataType,Scalar,Row,RowBatchBuilder,CreditKind};
        let schema=Arc::new(Schema::new(SchemaId::new(1),vec![Field::new(FieldId::new(1),"device",DataType::Utf8,false)]).unwrap());
        for topic in ["x".repeat(1025),"bad/+".into(),"bad/#".into()] {
            let mut cfg=MqttSinkConfig::demo("127.0.0.1",1883);cfg.topic=topic;
            assert!(MqttSink::bind(cfg,&MapSecretResolver::empty(),&TargetPolicy::allow("127.0.0.1",1883),IoDiagnostics::new()).unwrap()
                .with_action(Some(Box::new(sparrow_formats::action::ActionSpec::default()))).is_err());
        }
        for (value,valid) in [("测-a",true),("a/b",false),("a+",false),("#",false),("\0",false)] {
            let owner=MemoryOwner::new(ResourceBudget::compact());let diag=IoDiagnostics::new();
            let action=serde_json::from_value(serde_json::json!({"topic":["site/",{"$field":"device"},"/out"],"body":{"id":{"$field":"device"}}})).unwrap();
            let sink=MqttSink::bind(MqttSinkConfig::demo("127.0.0.1",1883),&MapSecretResolver::empty(),&TargetPolicy::allow("127.0.0.1",1883),diag.clone()).unwrap().with_action(Some(Box::new(action))).unwrap();
            let mut builder=RowBatchBuilder::new(schema.clone(),owner.clone(),CreditKind::Reservation,1,65536).unwrap();builder.push(Row{values:vec![Scalar::utf8(value)]}).unwrap();let batch=builder.finish().unwrap();
            let(left,right)=tokio::io::duplex(65536);let mut writer:super::super::io::MqttStream=Box::pin(left);let mut reader:super::super::io::MqttStream=Box::pin(right);
            assert_eq!(sink.publish_action(&batch,&batch.rows()[0],&mut writer,&CancellationToken::new()).await.unwrap(),valid);
            if valid {
                let Packet::Publish(packet)=read_packet(&mut MqttFramedReader::new(),&mut reader).await.unwrap() else {panic!("expected publish")};
                assert_eq!(packet.topic,"site/测-a/out");assert_eq!(serde_json::from_slice::<serde_json::Value>(&packet.payload).unwrap(),serde_json::json!({"id":"测-a"}));
            } else {assert_eq!(diag.snapshot().mqtt_dropped_bad,1);}
            drop(batch);assert_eq!(owner.usage().physical_bytes,0);
        }
    }
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
    pub fn with_action(mut self,action:Option<Box<sparrow_formats::action::ActionSpec>>)->Result<Self>{
        if action.as_ref().is_some_and(|a|a.single || !a.query.is_empty()) {
            return Err(ConnectorError::new(ErrorCode::InvalidArgument,"MQTT actions do not accept HTTP options"));
        }
        if action.as_ref().is_some_and(|a|a.topic.is_none()) &&
            (self.config.topic.is_empty() || self.config.topic.len()>1024 || self.config.topic.contains(['+','#','\0'])) {
            return Err(ConnectorError::new(ErrorCode::InvalidArgument,"MQTT action fallback topic must be valid and <=1024 bytes"));
        }
        self.action=action;Ok(self)
    }

    async fn publish_action(&self,batch:&RowBatch,row:&sparrow_model::Row,stream:&mut super::io::MqttStream,cancel:&CancellationToken)->Result<bool>{
        let action=self.action.as_ref().expect("action");
        let prepared=(||->sparrow_model::Result<_>{
            if batch.output_sequence().is_some(){return Err(sparrow_model::SparrowError::new(ErrorCode::UnsupportedRestore,"MQTT action has no reliable receipt"));}
            // CSV: its encoder scratch, then every output growth, is charged
            // before allocation; the message is bounded at 64KiB.
            let scratch=self.config.payload_format.as_csv().map_or(8192,|csv|csv.encode_scratch(row).max(8192));
            let mut lease=batch.lease().owner().acquire(sparrow_model::CreditKind::Reservation,scratch)?;
            // CSV sinks accept only `action.topic` (validated); the body is the CSV message.
            let body=match self.config.payload_format.as_csv() {
                Some(csv)=>csv.encode_message_bounded_with_capacity(batch.schema(),row,JsonLimits::default().max_bytes,|cap|lease.grow_to(cap.saturating_add(scratch)))
                    .inspect_err(|e|if e.code!=ErrorCode::ResourceExhausted {self.diag.csv_encode_error(&self.config.payload_format)})?,
                None=>action.encode(batch.schema(),std::slice::from_ref(row),false,JsonLimits::default().max_bytes,|cap|lease.grow_to(cap+8192))?,
            };
            let topic=if let Some(parts)=&action.topic {
                // A variable occupies one topic level; only configured literals
                // may introduce separators. No data-controlled wildcard routing.
                for part in parts.iter().filter(|p|!p.is_string()) {
                    let value=action.text(std::slice::from_ref(part),batch.schema(),row,1024)?;
                    if value.contains(['/', '+', '#', '\0']) {return Err(sparrow_model::SparrowError::new(ErrorCode::PolicyDenied,"dynamic topic field must stay within one topic level"));}
                }
                action.text(parts,batch.schema(),row,1024)?
            }else{
                if self.config.topic.len()>1024{return Err(sparrow_model::SparrowError::new(ErrorCode::BoundExceeded,"action fallback topic exceeds 1024 bytes"));}
                self.config.topic.clone()
            };
            if topic.is_empty() || topic.len()>1024 || topic.contains(['+','#','\0']) {
                return Err(sparrow_model::SparrowError::new(ErrorCode::PolicyDenied,"invalid expanded MQTT publish topic"));
            }
            // Codec holds payload + variable header + final frame at its peak;
            // include Vec geometric growth and bounded topic/text scratch.
            lease.grow_to(body.capacity().saturating_mul(5).saturating_add(16*1024))?;
            Ok((lease,body,topic))
        })();
        let (_lease,body,topic)=match prepared {Ok(value)=>value,Err(e)=>{
            self.diag.mqtt_dropped_bad.fetch_add(1,Ordering::Relaxed);
            self.diag.observation.health(false,HealthState::Failed,"mqtt_action_failed",Some(e.code));return Ok(false);
        }};
        let packet=Packet::Publish(Publish{dup:false,qos:0,retain:false,topic,packet_id:None,payload:body});
        tokio::select! {biased;_=cancel.cancelled()=>return Err(ConnectorError::new(ErrorCode::JobFailed,"MQTT action cancelled")),
            result=tokio::time::timeout(self.config.connect_timeout,write_packet(stream,&packet))=>result.map_err(|_|ConnectorError::new(ErrorCode::Internal,"MQTT action write timeout"))??,
        }
        self.diag.mqtt_decoded.fetch_add(1,Ordering::Relaxed);self.diag.observation.progress(false,1);Ok(true)
    }

    pub fn bind(
        config: MqttSinkConfig,
        secrets: &dyn SecretResolver,
        policy: &TargetPolicy,
        diag: Arc<IoDiagnostics>,
    ) -> Result<Self> {
        config.validate(secrets, policy)?;
        Ok(Self { config, diag, action:None })
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
                                if self.action.is_some() {
                                    if !Box::pin(self.publish_action(&batch,row,&mut stream,cancel)).await? {all_encoded=false;}
                                    next_ping=tokio::time::Instant::now()+ping;
                                    continue;
                                }
                                let started=std::time::Instant::now();
                                // Encoder scratch and output are charged to the batch owner
                                // before allocation and bounded by max_bytes; the lease
                                // lives until this row's PUBLISH was written.
                                let encoded=crate::scratch::encode_row_charged(batch.lease().owner(),&self.config.payload_format,schema,row,limits.max_bytes);
                                self.diag.observation.record(Latency::Encode,started.elapsed());
                                let (body, _encode_credit) = match encoded {
                                    Ok(encoded) => encoded,
                                    Err(rejected) => {
                                        all_encoded=false;
                                        let code=if rejected==crate::scratch::EncodeRejected::Budget {ErrorCode::ResourceExhausted} else {
                                            if rejected==crate::scratch::EncodeRejected::Bad {self.diag.csv_encode_error(&self.config.payload_format);}
                                            ErrorCode::CodecViolation
                                        };
                                        self.diag.observation.health(false,HealthState::Failed,"mqtt_sink_encode_failed",Some(code));
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

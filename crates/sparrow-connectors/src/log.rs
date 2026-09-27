use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use sparrow_formats::encode_json_row;
use sparrow_model::{InflightCounter, RestoreClaim, RowBatch};
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use sparrow_io::observed::Receiver as ObservedReceiver;
use sparrow_model::observation::HealthState;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct LogSinkConfig {
    pub restore: RestoreClaim,
}

impl Default for LogSinkConfig {
    fn default() -> Self {
        Self {
            restore: RestoreClaim::None,
        }
    }
}

impl LogSinkConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::LOG_SINK
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore)
    }
}

/// Writes encoded JSON lines to an in-memory ring (bounded) and stderr.
pub struct LogSink {
    pub diag: Arc<IoDiagnostics>,
    lines: Arc<Mutex<Vec<String>>>,
    max_lines: usize,
    action: Option<Box<sparrow_formats::action::ActionSpec>>,
}

impl LogSink {
    /// Production logging does not retain a second, unobserved in-memory copy
    /// of telemetry. `new` keeps its explicitly requested debug capture API.
    pub fn unbuffered(diag:Arc<IoDiagnostics>)->Self {
        let mut sink=Self::new(diag,1);sink.max_lines=0;sink
    }
    pub fn new(diag: Arc<IoDiagnostics>, max_lines: usize) -> Self {
        Self {
            diag,
            lines: Arc::new(Mutex::new(Vec::new())),
            max_lines: max_lines.max(1),
            action:None,
        }
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("log").clone()
    }
    pub fn with_action(mut self,action:Option<Box<sparrow_formats::action::ActionSpec>>)->Self {self.action=action;self}

    pub fn shared_lines(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.lines)
    }

    /// Connector SDK flush. Log lines are already in the ring; this is a
    /// barrier no-op so experimental checkpoint can wait on sink flush.
    pub fn flush(&self) -> Result<()> {
        drop(self.lines.lock().expect("log"));
        Ok(())
    }

    pub async fn run(
        self,
        rx: impl Into<ObservedReceiver<RowBatch>>,
        cancel: CancellationToken,
        outbox: Option<Arc<InflightCounter>>,
    ) {
        let mut rx=rx.into();
        let _lifecycle=self.diag.observation.lifecycle(false);
        self.diag.observation.health(false,HealthState::Ready,"log_writer_ready",None);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                next = rx.recv() => {
                    match next {
                        Some(batch) => {
                            let mut delivered=self.diag.observation.delivery_guard(batch.num_rows(),batch.tracked_bytes(),batch.origin());
                            let ok=self.write_batch(&batch);
                            if ok {delivered.complete();self.diag.observation.progress(false,batch.num_rows());}
                            else{self.diag.observation.health(false,HealthState::Failed,"log_encode_failed",Some(sparrow_model::ErrorCode::CodecViolation));}
                            if let Some(o) = &outbox {
                                if ok{o.ack();}else{o.fail();}
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        rx.close();while let Ok(_batch)=rx.discard_next(){if let Some(outbox)=&outbox{outbox.fail();}}
    }

    fn write_batch(&self, batch: &RowBatch) -> bool {
        if self.action.is_some() && batch.output_sequence().is_some(){return false;}
        let mut ok=true;
        let schema = batch.schema();
        let mut guard = self.lines.lock().expect("log");
        for row in batch.rows() {
            let mut credit=None;
            let encoded=if let Some(action)=&self.action {
                if batch.output_sequence().is_some(){Err(sparrow_model::SparrowError::new(sparrow_model::ErrorCode::UnsupportedRestore,"Log action is not replayable"))}
                else{match batch.lease().owner().acquire(sparrow_model::CreditKind::Reservation,8192){
                    Ok(lease)=>{credit=Some(lease);action.encode(schema,std::slice::from_ref(row),false,64*1024,|cap|credit.as_mut().expect("credit").grow_to(cap+8192))},
                    Err(e)=>Err(e),
                }}
            }else{encode_json_row(schema,row)};
            if let Ok(bytes) = encoded {
                let line = String::from_utf8_lossy(&bytes);
                eprintln!("sparrow-log {line}");
                if self.max_lines>0 {
                    if guard.len()>=self.max_lines {guard.remove(0);}
                    guard.push(line.into_owned());
                }
                self.diag.log_written.fetch_add(1, Ordering::Relaxed);
            }else{ok=false;}
        }
        ok
    }
}

#[cfg(test)]
mod action_tests {
    use super::*;
    use sparrow_model::{MemoryOwner,ResourceBudget,Schema,Field,DataType,Row,RowBatchBuilder,Scalar,CreditKind};
    #[tokio::test]
    async fn actions_log_production_has_no_retained_ring_and_cancel_refunds() {
        let owner=MemoryOwner::new(ResourceBudget::compact());let diag=IoDiagnostics::new();
        let schema=Arc::new(Schema::new(1,vec![Field::new(1,"n",DataType::Int64,false)]).unwrap());
        let mut builder=RowBatchBuilder::new(schema,owner.clone(),CreditKind::Reservation,1,1024).unwrap();builder.push(Row{values:vec![Scalar::Int64(9)]}).unwrap();let batch=builder.finish().unwrap();
        let sink=LogSink::unbuffered(diag.clone()).with_action(Some(Box::new(sparrow_formats::action::ActionSpec{body:Some(serde_json::json!({"v":{"$field":"n"}})),..Default::default()})));
        assert!(sink.write_batch(&batch));assert!(sink.lines().is_empty());assert_eq!(diag.snapshot().log_written,1);
        let(tx,rx)=tokio::sync::mpsc::channel(1);tx.send(batch).await.unwrap();drop(tx);let cancel=CancellationToken::new();cancel.cancel();let outbox=Arc::new(InflightCounter::new());outbox.enqueue();
        // Receiver cancellation wins deterministically, no hidden delivery.
        sink.run(rx,cancel,Some(outbox.clone())).await;assert_eq!(outbox.pending(),0);assert_eq!(owner.usage().physical_bytes,0);
    }
}

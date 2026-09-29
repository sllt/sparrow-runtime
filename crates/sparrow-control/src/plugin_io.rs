//! Copy-only external I/O. Accepted means volatile admission/transport
//! completion, never checkpoint or exactly-once receipt. No automatic retry.
use sparrow_connectors::IoDiagnostics;
use sparrow_expr::plugins::{extension as ext, Extension};
use sparrow_io::observed;
use sparrow_model::{
    observation::{HealthState, OriginSpan},
    CreditKind, ErrorCode, InflightCounter, MemoryOwner, Result, RowBatch, RowBatchBuilder, Schema,
    SparrowError,
};
use sparrow_runtime::{IngressEvent, StreamControl};
use std::sync::{atomic::Ordering, Arc};
use tokio_util::sync::CancellationToken;

fn failed(diag: &IoDiagnostics, source: bool, error: &SparrowError, cancel: &CancellationToken) {
    if error.code == ErrorCode::Cancelled && cancel.is_cancelled() {
        return;
    }
    diag.plugin_failed.fetch_add(1, Ordering::Relaxed);
    diag.observation.health(
        source,
        HealthState::Failed,
        "external_plugin_failed",
        Some(error.code),
    );
    cancel.cancel();
}
fn send(
    handle: &tokio::runtime::Handle,
    tx: &observed::Sender<IngressEvent>,
    event: IngressEvent,
    cancel: &CancellationToken,
) -> Result<()> {
    handle.block_on(async {tokio::select! {biased;
        _=cancel.cancelled()=>Err(SparrowError::new(ErrorCode::Cancelled,"external Source cancelled")),
        result=tx.send(event)=>result.map_err(|_|SparrowError::new(ErrorCode::Cancelled,"external Source inbox closed")),
    }})
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn source(
    handle: tokio::runtime::Handle,
    extension: Arc<Extension>,
    config: serde_json::Value,
    schema: Schema,
    tx: observed::Sender<IngressEvent>,
    cancel: CancellationToken,
    owner: Arc<MemoryOwner>,
    diag: Arc<IoDiagnostics>,
    graph: bool,
    batch_rows: usize,
) -> tokio::task::JoinHandle<Result<()>> {
    let runtime = handle.clone();
    handle.spawn_blocking(move || {
        let _life=diag.observation.lifecycle(true);
        let result=(|| {
            let d=extension.declaration();
            if !ext::matches_schema(&d.output,&schema,false) {return Err(SparrowError::new(ErrorCode::InvalidSchema,"external Source schema mismatch"));}
            let _scratch=owner.acquire(CreditKind::Reservation,ext::scratch_bytes(d))?;
            let mut session=extension.open(&config,cancel.clone())?;
            diag.observation.health(true,HealthState::Ready,"external_source_open",None);
            let schema=Arc::new(schema);
            let run=(|| {
                while !cancel.is_cancelled() {
                    diag.plugin_polls.fetch_add(1,Ordering::Relaxed);
                    match session.poll_max(batch_rows.min(d.limits.max_rows))? {
                        ext::Reply::Data {rows,watermark}=> {
                            let at=OriginSpan::at(std::time::Instant::now());
                            let rows=ext::into_rows(rows,&d.output,d.limits.max_rows)?;
                            let count=rows.len();
                            if count>0 {
                                let mut batch=RowBatchBuilder::new(schema.clone(),owner.clone(),CreditKind::Reservation,d.limits.max_rows,owner.budget().reservation_bytes)?;
                                for row in rows {let bytes=row.resident_bytes().saturating_add(64);batch.push_accounted(row,bytes)?;}
                                send(&runtime,&tx,IngressEvent::admitted_batch(batch.finish()?.with_origin(at))?,&cancel)?;
                            }
                            if let Some(wm_micros)=watermark {
                                send(&runtime,&tx,IngressEvent::Control(StreamControl::Watermark {input:sparrow_model::InputId::SINGLE.raw(),wm_micros}),&cancel)?;
                            }
                            session.accepted()?;
                            diag.plugin_source_rows.fetch_add(count as u64,Ordering::Relaxed);
                            diag.observation.progress(true,count);
                        }
                        ext::Reply::Idle {retry_after_ms}=>runtime.block_on(async {tokio::select!{biased;_=cancel.cancelled()=>{},_=tokio::time::sleep(std::time::Duration::from_millis(retry_after_ms))=>{}}}),
                        ext::Reply::End=> {
                            // End explicitly claims finite completion. Linear
                            // ingress closes by channel EOF, graph by punctuation.
                            send(&runtime,&tx,IngressEvent::Control(StreamControl::Watermark {input:sparrow_model::InputId::SINGLE.raw(),wm_micros:sparrow_connectors::FileContract::TERMINAL_WM_MICROS}),&cancel)?;
                            if graph {send(&runtime,&tx,IngressEvent::Control(StreamControl::EndOfInput),&cancel)?;}
                            break;
                        }
                        _=>unreachable!("validated by host Session"),
                    }
                }
                Ok(())
            })();
            let close=session.close();
            run.and(close)
        })();
        if let Err(error)=&result {failed(&diag,true,error,&cancel);}
        result
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sink(
    handle: tokio::runtime::Handle,
    extension: Arc<Extension>,
    config: serde_json::Value,
    mut rx: observed::Receiver<RowBatch>,
    cancel: CancellationToken,
    owner: Arc<MemoryOwner>,
    diag: Arc<IoDiagnostics>,
    outbox: Option<Arc<InflightCounter>>,
) -> tokio::task::JoinHandle<()> {
    let runtime = handle.clone();
    handle.spawn_blocking(move || {
        let _life = diag.observation.lifecycle(false);
        let result = (|| {
            let d = extension.declaration();
            let _scratch = owner.acquire(CreditKind::Reservation, ext::scratch_bytes(d))?;
            let mut session = extension.open(&config, cancel.clone())?;
            diag.observation
                .health(false, HealthState::Ready, "external_sink_open", None);
            let run = (|| {
                loop {
                    let batch = runtime.block_on(async {
                        tokio::select! {biased;_=cancel.cancelled()=>None,batch=rx.recv()=>batch}
                    });
                    let Some(batch) = batch else { break };
                    if !ext::matches_schema(&d.input, batch.schema(), true) {
                        return Err(SparrowError::new(
                            ErrorCode::InvalidSchema,
                            "external Sink schema mismatch",
                        ));
                    }
                    // A large valid row must not be made invalid by grouping.
                    // max_rows is a cap, not an implicit batch/linger policy.
                    for row in batch.rows() {
                        session.push(vec![ext::from_row(row, d.limits.max_frame_bytes)?])?;
                        diag.plugin_sink_rows.fetch_add(1, Ordering::Relaxed);
                        diag.observation.progress(false, 1);
                    }
                    if let Some(counter) = &outbox {
                        counter.ack();
                    }
                }
                Ok(())
            })();
            let close = session.close();
            run.and(close)
        })();
        if let Err(error) = result {
            if let Some(counter) = outbox {
                counter.fail();
            }
            failed(&diag, false, &error, &cancel);
        }
    })
}

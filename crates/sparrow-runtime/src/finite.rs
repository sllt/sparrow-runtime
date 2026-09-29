//! Independent finite-query Kernel. No live connectors, persistence or actions.
use crate::{GraphInput, JobRequest, Kernel, KernelOptions, MailboxConfig, SharedCapture};
use sparrow_model::{
    CreditKind, ErrorCode, MemoryLease, OperatorId, ResourceBudget, Result, Row, RowBatch, Schema,
    SparrowError,
};
use sparrow_plan::{PhysicalPlan, PhysicalStage};
use std::{collections::HashMap, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug)]
pub struct FiniteLimits {
    pub input_rows: usize,
    pub output_rows: usize,
    pub output_bytes: usize,
    pub work_units: u64,
    pub timeout_ms: u64,
}
impl Default for FiniteLimits {
    fn default() -> Self {
        Self {
            input_rows: 1024,
            output_rows: 256,
            output_bytes: 512 * 1024,
            work_units: 1_000_000,
            timeout_ms: 2000,
        }
    }
}
impl FiniteLimits {
    pub fn validate(&self) -> Result<()> {
        if !(1..=4096).contains(&self.input_rows)
            || !(1..=4096).contains(&self.output_rows)
            || !(1..=1024 * 1024).contains(&self.output_bytes)
            || !(1..=10_000_000).contains(&self.work_units)
            || !(10..=30_000).contains(&self.timeout_ms)
        {
            return Err(bound("invalid finite query limits"));
        }
        Ok(())
    }
}
pub struct FiniteResult {
    pub future_dropped: u64,
    pub schema: Schema,
    pub batches: Vec<RowBatch>,
    pub input_rows: usize,
    pub output_rows: usize,
    _metadata: MemoryLease,
}
impl FiniteResult {
    pub fn owner(&self) -> std::sync::Arc<sparrow_model::MemoryOwner> {
        self._metadata.owner().clone()
    }
}
fn bound(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::BoundExceeded, s)
}

/// Blocking boundary: call on a bounded worker, not an async executor thread.
/// On cancellation/timeout the worker cancels AND joins all Kernel tasks before
/// its admission permit can be released. Returned batches retain their credits.
pub fn execute(
    plan: PhysicalPlan,
    mut inputs: HashMap<OperatorId, Vec<Row>>,
    limits: FiniteLimits,
    cancel: CancellationToken,
) -> Result<FiniteResult> {
    limits.validate()?;
    if plan.has_plugins(){return Err(bound("finite queries reject plugins: native is not preemptible; script aggregate query work budgeting is not enabled"));}
    if plan.stages.len() > 32
        || plan
            .stages
            .iter()
            .filter(|s| matches!(s, PhysicalStage::CaptureSink { .. }))
            .count()
            != 1
        || !plan.side_outputs.is_empty()
        || plan
            .edges
            .as_ref()
            .is_some_and(|edges| edges.iter().any(|e| e.best_effort))
        || plan.stages.iter().any(|s| {
            !matches!(
                s,
                PhysicalStage::MemorySource { .. }
                    | PhysicalStage::CaptureSink { .. }
                    | PhysicalStage::Transform { .. }
                    | PhysicalStage::Analysis { .. }
                    | PhysicalStage::WindowAgg { .. }
            )
        })
        || plan.has_processing_time_state()
    {
        return Err(bound("finite queries support one capture sink, pure transforms, ET/count windows and bounded analysis; no live/PT/lookup/side I/O"));
    }
    let sources: Vec<_> = plan
        .stages
        .iter()
        .filter_map(|s| match s {
            PhysicalStage::MemorySource { operator, .. } => Some(*operator),
            _ => None,
        })
        .collect();
    if sources.is_empty()
        || sources.len() > 4
        || inputs.len() != sources.len()
        || sources.iter().any(|id| !inputs.contains_key(id))
    {
        return Err(bound("finite query inputs must exactly match 1..4 sources"));
    }
    let count = inputs.values().map(Vec::len).sum::<usize>();
    let bytes = inputs.values().flatten().fold(0usize, |n, r| {
        n.saturating_add(r.resident_bytes()).saturating_add(64)
    });
    if count > limits.input_rows || bytes > 2 * 1024 * 1024 {
        return Err(bound("finite query input rows/decoded bytes exceeded"));
    }
    let schema = plan
        .stages
        .iter()
        .find_map(|s| match s {
            PhysicalStage::CaptureSink { schema, .. } => Some(schema.clone()),
            _ => None,
        })
        .unwrap();
    let budget = ResourceBudget {
        max_rows: 128,
        max_state_keys: 1024,
        work_units: 10_000,
        ..ResourceBudget::compact()
    };
    let kernel = Kernel::new_with_job_budget(
        KernelOptions {
            budget,
            mailbox: MailboxConfig {
                max_items: 2,
                max_bytes: 16 * 1024,
            },
            worker_threads: 1,
            rows_per_batch: 8,
        },
        budget,
    )?;
    let admission = kernel.prepare_source_admission(plan.pipeline)?;
    let owner = admission.owner();
    let _inputs = owner.acquire(CreditKind::Retention, bytes.saturating_add(512))?;
    let metadata = owner.acquire(
        CreditKind::Retention,
        limits
            .output_rows
            .saturating_mul(std::mem::size_of::<RowBatch>())
            .saturating_mul(2)
            .saturating_add(256),
    )?;
    let (tx, mut rx) = sparrow_io::observed::channel(2);
    let graph = plan.edges.is_some();
    let event_time = plan.has_event_time_window();
    let mut request =
        JobRequest::new(plan, vec![], SharedCapture::disabled()).with_source_admission(admission);
    if graph {
        for id in sources {
            request.graph_inputs.insert(
                id,
                GraphInput {
                    rows: inputs.remove(&id).unwrap(),
                    ..Default::default()
                },
            );
        }
    } else {
        request.rows = inputs.remove(&sources[0]).unwrap();
        if event_time {
            request
                .trailing_controls
                .push(crate::StreamControl::Watermark {
                    input: 0,
                    wm_micros: i64::MAX,
                });
        }
    }
    request.live_out = Some(tx);
    request.work_lifetime = Some(limits.work_units);
    let handle = kernel.submit(request)?;
    let job_cancel = handle.cancellation();
    let batches=kernel.block_on(async {
        let mut wait=Box::pin(handle.wait());let mut done=None;let mut closed=false;
        let deadline=tokio::time::sleep(Duration::from_millis(limits.timeout_ms));tokio::pin!(deadline);
        let mut output=Vec::new();let mut row_count=0usize;let mut total=0usize;
        loop {
            if closed&&done.is_some(){break;}
            let failure=tokio::select!{biased;
                _=cancel.cancelled()=>Some(SparrowError::new(ErrorCode::Cancelled,"finite query cancelled")),
                _=&mut deadline=>Some(SparrowError::new(ErrorCode::Cancelled,"finite query deadline exceeded")),
                result=&mut wait,if done.is_none()=>{done=Some(result);None},
                batch=rx.recv(),if !closed=>{
                    if let Some(batch)=batch {
                        row_count=row_count.saturating_add(batch.num_rows());total=total.saturating_add(batch.tracked_bytes());
                        if row_count>limits.output_rows||total>limits.output_bytes{Some(bound("finite query output rows/bytes exceeded; no partial successful response"))}
                        else{output.push(batch);None}
                    }else{closed=true;None}
                },
            };
            if let Some(error)=failure {job_cancel.cancel();if done.is_none(){let _=wait.await;}return Err(error);}
        }
        done.unwrap()?;Ok(output)
    })?;
    let output_rows = batches.iter().map(RowBatch::num_rows).sum();
    Ok(FiniteResult {
        future_dropped: kernel.metrics.snapshot().future_dropped,
        schema,
        batches,
        input_rows: count,
        output_rows,
        _metadata: metadata,
    })
}

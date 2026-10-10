//! Independently admitted, finite, side-effect-free query execution.
use serde::Deserialize;
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
pub(crate) static QUERY_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    stream: String,
    rows: Vec<Box<serde_json::value::RawValue>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    graph: Option<sparrow_plan::GraphSpec>,
    inputs: Vec<Input>,
    #[serde(default)]
    limits: Limits,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    input_rows: Option<usize>,
    output_rows: Option<usize>,
    output_bytes: Option<usize>,
    work_units: Option<u64>,
    timeout_ms: Option<u64>,
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}
/// The response body owns both its bytes credit and query slot until sent or
/// dropped; a slow client cannot recycle admission while pinning output memory.
pub struct QueryOutput {
    payload: Vec<u8>,
    _memory: sparrow_model::MemoryLease,
    _permit: tokio::sync::SemaphorePermit<'static>,
}
impl AsRef<[u8]> for QueryOutput {
    fn as_ref(&self) -> &[u8] {
        &self.payload
    }
}
pub async fn execute(store: Arc<crate::Store>, bytes: Vec<u8>) -> Result<QueryOutput> {
    let permit = QUERY_SLOTS.try_acquire().map_err(|_| {
        SparrowError::new(
            ErrorCode::ResourceExhausted,
            "finite query concurrency is limited to two; retry later",
        )
    })?;
    if bytes.len() > 64 * 1024 {
        return Err(invalid("query body exceeds 64KiB"));
    }
    let cancel = CancellationToken::new();
    let guard = CancelOnDrop(cancel.clone());
    let worker = tokio::task::spawn_blocking(move || {
        // Reject duplicate keys before serde_json Value can collapse them.
        sparrow_formats::decode_dynamic_json(
            &bytes,
            &sparrow_formats::JsonLimits {
                max_bytes: 64 * 1024,
                max_depth: 16,
            },
        )?;
        let request: Request =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid finite query request"))?;
        let mut limits = sparrow_runtime::finite::FiniteLimits::default();
        if let Some(v) = request.limits.input_rows {
            limits.input_rows = v;
        }
        if let Some(v) = request.limits.output_rows {
            limits.output_rows = v;
        }
        if let Some(v) = request.limits.output_bytes {
            limits.output_bytes = v;
        }
        if let Some(v) = request.limits.work_units {
            limits.work_units = v;
        }
        if let Some(v) = request.limits.timeout_ms {
            limits.timeout_ms = v;
        }
        limits.validate()?;
        if !(1..=4).contains(&request.inputs.len())
            || request.sql.as_ref().is_some_and(|sql| sql.len() > 8192)
        {
            return Err(invalid("query requires 1..4 inputs and SQL <=8KiB"));
        }
        let mut catalog = sparrow_plan::Catalog::new();
        catalog.plugins=store.plugins();
        let mut names = BTreeSet::new();
        for input in &request.inputs {
            if input.stream.len() > 128 || !names.insert(&input.stream) {
                return Err(invalid("invalid/duplicate query stream name"));
            }
            if request
                .graph
                .as_ref()
                .is_some_and(|g| g.catalog.iter().any(|s| s.name == input.stream))
            {
                continue;
            }
            catalog.insert(
                &input.stream,
                crate::stream_to_schema(&store.get_stream(&input.stream)?)?,
            );
        }
        if cancel.is_cancelled() {
            return Err(SparrowError::new(
                ErrorCode::Cancelled,
                "finite query cancelled before execution",
            ));
        }
        let bound = match (request.sql, request.graph) {
            (Some(sql), None) => sparrow_sql::bind_sql(&sql, &catalog, 1.into(), 1.into())?,
            (None, Some(graph)) => sparrow_plan::bind_graph(&graph, &catalog)?,
            _ => return Err(invalid("query requires exactly one of sql/graph")),
        };
        let plan = sparrow_plan::physicalize(&bound, &Default::default());
        if plan.has_external_plugins() {
            return Err(invalid("finite queries do not execute trusted native Transform programs"));
        }
        let mut supplied = HashMap::new();
        for input in request.inputs {
            if supplied.insert(input.stream, input.rows).is_some() {
                return Err(invalid("duplicate query stream input"));
            }
        }
        let expected: BTreeSet<_> = plan
            .stages
            .iter()
            .filter_map(|s| match s {
                sparrow_plan::PhysicalStage::MemorySource { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect();
        if supplied.keys().cloned().collect::<BTreeSet<_>>() != expected {
            return Err(invalid(
                "query inputs must exactly match source stream names",
            ));
        }
        let mut inputs = HashMap::new();
        let mut count = 0usize;
        let mut decoded = 0usize;
        for stage in &plan.stages {
            if let sparrow_plan::PhysicalStage::MemorySource {
                operator,
                name,
                schema,
            } = stage
            {
                let mut rows = Vec::new();
                for value in &supplied[name] {
                    count += 1;
                    if count > limits.input_rows {
                        return Err(invalid("query input row limit exceeded"));
                    }
                    let row = sparrow_formats::decode_json_row(
                        schema,
                        value.get().as_bytes(),
                        &Default::default(),
                    )?;
                    decoded = decoded.saturating_add(row.resident_bytes());
                    if decoded > 2 * 1024 * 1024 {
                        return Err(invalid("query decoded input limit exceeded"));
                    }
                    rows.push(row);
                }
                inputs.insert(*operator, rows);
            }
        }
        let result = sparrow_runtime::finite::execute(plan, inputs, limits, cancel)?;
        let cap = limits.output_bytes.saturating_add(1024);
        let memory = result.owner().acquire(
            sparrow_model::CreditKind::Reservation,
            cap.saturating_mul(2),
        )?;
        let mut response = Vec::with_capacity(cap);
        response.extend_from_slice(b"{\"rows\":[");
        let mut encoded = 0usize;
        let mut first = true;
        for batch in &result.batches {
            let mut lease = batch
                .lease()
                .owner()
                .acquire(sparrow_model::CreditKind::Reservation, 256)?;
            let payload = sparrow_formats::encode_json_batch_bounded_with_capacity(
                &result.schema,
                batch.rows(),
                limits.output_bytes.saturating_sub(encoded),
                |capacity| lease.grow_to(capacity.saturating_mul(2).saturating_add(512)),
            )?;
            encoded = encoded.saturating_add(payload.len());
            if batch.num_rows() > 0 {
                if !first {
                    response.push(b',');
                }
                first = false;
                response.extend_from_slice(&payload[1..payload.len() - 1]);
            }
        }
        response.extend_from_slice(format!("],\"input_rows\":{},\"output_rows\":{},\"future_dropped\":{},\"complete\":true,\"execution\":\"independent_bounded_kernel\",\"side_effects\":false,\"recovery\":\"none\",\"certified\":false}}",result.input_rows,result.output_rows,result.future_dropped).as_bytes());
        if response.len() > cap {
            return Err(invalid("query response exceeded its admitted bound"));
        }
        Ok(QueryOutput {
            payload: response,
            _memory: memory,
            _permit: permit,
        })
    });
    let result = worker
        .await
        .map_err(|_| SparrowError::new(ErrorCode::JobFailed, "finite query worker failed"))?;
    drop(guard);
    result
}

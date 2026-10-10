//! K5.4 `/v1/preview`: bounded, side-effect-free, controlled-time debugging.
//!
//! Shares the finite execution admission with `/v1/query` (two at a time in
//! total). A real PipelineSpec is never run with its connectors: the server
//! binds only the plan, swaps the I/O for an in-memory source and capture,
//! and lists every field that played no part.
use serde::Deserialize;
use serde_json::value::RawValue;
use sparrow_model::{ErrorCode, Result, SparrowError};
use sparrow_runtime::preview::{PreviewEvent, PreviewLimits};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub const MAX_PREVIEW_BODY: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(default)]
    spec: Option<Box<RawValue>>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    stream: Option<String>,
    #[serde(default)]
    graph: Option<sparrow_plan::GraphSpec>,
    /// Logical start of processing time; never read from the host clock.
    #[serde(default)]
    start_micros: i64,
    events: Vec<Event>,
    #[serde(default)]
    limits: Limits,
}
/// Flat struct (not an internally tagged enum) so `row` stays a RawValue and
/// keeps exact number literals.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    row: Option<Box<RawValue>>,
    #[serde(default)]
    to_micros: Option<i64>,
    #[serde(default)]
    micros: Option<i64>,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    events: Option<usize>,
    output_rows: Option<usize>,
    output_bytes: Option<usize>,
    timeout_ms: Option<u64>,
    work_units: Option<u64>,
}
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}

const NOT_VERIFIED: &[&str] = &[
    "source/sink connections and credentials",
    "delivery guarantees and outbox",
    "checkpoint, recovery and restore",
    "wall-clock scheduling and backpressure",
];

pub async fn execute(store: Arc<crate::Store>, bytes: Vec<u8>) -> Result<Vec<u8>> {
    let permit = crate::query::QUERY_SLOTS.try_acquire().map_err(|_| {
        SparrowError::new(ErrorCode::ResourceExhausted, "finite execution (query + preview) is limited to two; retry later")
    })?;
    if bytes.len() > MAX_PREVIEW_BODY {
        return Err(invalid("preview body exceeds 256KiB"));
    }
    let cancel = CancellationToken::new();
    let guard = DropCancel(cancel.clone());
    let worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        run(&store, &bytes, cancel)
    });
    let out = worker.await.map_err(|_| SparrowError::new(ErrorCode::JobFailed, "preview worker failed"))?;
    drop(guard);
    out
}
struct DropCancel(CancellationToken);
impl Drop for DropCancel {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn run(store: &crate::Store, bytes: &[u8], cancel: CancellationToken) -> Result<Vec<u8>> {
    sparrow_formats::decode_dynamic_json(bytes, &sparrow_formats::JsonLimits { max_bytes: MAX_PREVIEW_BODY, max_depth: 16 })?;
    let req: Request = serde_json::from_slice(bytes).map_err(|e| invalid(&format!("invalid preview request: {e}")))?;
    let mut limits = PreviewLimits::default();
    let l = &req.limits;
    if let Some(v) = l.events { limits.events = v; }
    if let Some(v) = l.output_rows { limits.output_rows = v; }
    if let Some(v) = l.output_bytes { limits.output_bytes = v; }
    if let Some(v) = l.timeout_ms { limits.timeout_ms = v; }
    if let Some(v) = l.work_units { limits.work_units = v; }
    limits.validate()?;
    if req.events.is_empty() || req.events.len() > limits.events {
        return Err(SparrowError::new(ErrorCode::BoundExceeded, "preview needs 1..limits.events events"));
    }
    let mut ignored: Vec<&'static str> = Vec::new();
    let plan = match (&req.spec, &req.sql, &req.graph) {
        (Some(raw), None, None) if req.stream.is_none() => {
            let spec = crate::PipelineSpec::from_json(raw.get().as_bytes())?;
            ignored.extend(["source", "sink", "delivery", "recovery"]);
            if spec.graph_io.is_some() { ignored.push("graph_io"); }
            if spec.checkpoint.is_some() || spec.checkpoint_dir.is_some() { ignored.push("checkpoint"); }
            if !spec.external_lookups.is_empty() { ignored.push("external_lookups"); }
            crate::validate::bind_plan_with_store(store, &spec, "preview", 0)?
        }
        (None, Some(sql), None) => {
            if sql.len() > 8192 { return Err(invalid("preview SQL exceeds 8KiB")); }
            let catalog = crate::validate::binder_catalog(store)?;
            let bound = sparrow_sql::bind_sql(sql, &catalog, 1.into(), 1.into())?;
            sparrow_plan::physicalize(&bound, &Default::default())
        }
        (None, None, Some(graph)) if req.stream.is_none() => {
            let catalog = crate::validate::binder_catalog(store)?;
            sparrow_plan::physicalize(&sparrow_plan::bind_graph(graph, &catalog)?, &Default::default())
        }
        _ => return Err(invalid("preview requires exactly one of spec / sql / graph")),
    };
    sparrow_runtime::preview::check_plan(&plan)?;
    let (source, schema) = match plan.stages.first() {
        Some(sparrow_plan::PhysicalStage::MemorySource { name, schema, .. }) => (name.clone(), schema.clone()),
        _ => return Err(invalid("preview plan has no source")),
    };
    if req.stream.as_ref().is_some_and(|s| s != &source) {
        return Err(invalid("stream does not match the plan's source"));
    }
    let mut events = Vec::with_capacity(req.events.len());
    let mut decoded = 0usize;
    for e in req.events {
        let only = |ok: bool| if ok { Ok(()) } else { Err(invalid("event has fields that do not belong to its type")) };
        events.push(match e.kind.as_str() {
            "data" => {
                only(e.to_micros.is_none() && e.micros.is_none())?;
                if e.source.as_ref().is_some_and(|s| s != &source) {
                    return Err(invalid("data event names an unknown source"));
                }
                let raw = e.row.ok_or_else(|| invalid("data event needs row"))?;
                let row = sparrow_formats::decode_json_row(&schema, raw.get().as_bytes(), &Default::default())?;
                decoded = decoded.saturating_add(row.resident_bytes());
                if decoded > 2 * 1024 * 1024 {
                    return Err(SparrowError::new(ErrorCode::BoundExceeded, "preview decoded input exceeds 2MiB"));
                }
                PreviewEvent::Data(row)
            }
            "advance_clock" => {
                only(e.row.is_none() && e.source.is_none() && e.micros.is_none())?;
                PreviewEvent::AdvanceClock(e.to_micros.ok_or_else(|| invalid("advance_clock needs to_micros"))?)
            }
            "watermark" => {
                only(e.row.is_none() && e.to_micros.is_none())?;
                if e.source.as_ref().is_some_and(|s| s != &source) {
                    return Err(invalid("watermark names an unknown source"));
                }
                PreviewEvent::Watermark(e.micros.ok_or_else(|| invalid("watermark needs micros"))?)
            }
            "eof" => {
                only(e.row.is_none() && e.to_micros.is_none() && e.micros.is_none())?;
                PreviewEvent::Eof
            }
            _ => return Err(invalid("event type must be data/advance_clock/watermark/eof")),
        });
    }
    let event_time = plan.has_event_time_window();
    let processing_time = plan.has_processing_time_state();
    let result = sparrow_runtime::preview::execute(plan, req.start_micros, events, limits, cancel)?;
    // Hand-built JSON so every row keeps its exact encoded literal.
    let mut out = Vec::with_capacity(4096);
    out.extend_from_slice(b"{\"schema\":");
    out.extend_from_slice(&serde_json::to_vec(&result.schema.fields.iter().map(|f| serde_json::json!({"name": f.name, "type": f.data_type.name(), "nullable": f.nullable})).collect::<Vec<_>>()).unwrap_or_default());
    out.extend_from_slice(b",\"steps\":[");
    let mut encoded = 0usize;
    for (i, step) in result.steps.iter().enumerate() {
        if i > 0 { out.push(b','); }
        let n: usize = step.batches.iter().map(|b| b.num_rows()).sum();
        out.extend_from_slice(format!("{{\"index\":{i},\"clock_micros\":{},\"watermark_micros\":{},\"output_rows\":{n},\"rows\":[", step.clock_micros, step.watermark_micros).as_bytes());
        let mut first = true;
        for b in &step.batches {
            if b.num_rows() == 0 { continue; }
            let payload = sparrow_formats::encode_json_batch_bounded_with_capacity(&result.schema, b.rows(), limits.output_bytes.saturating_sub(encoded), |_| Ok(()))?;
            encoded = encoded.saturating_add(payload.len());
            if !first { out.push(b','); }
            first = false;
            out.extend_from_slice(&payload[1..payload.len() - 1]);
        }
        out.extend_from_slice(b"]}");
    }
    let last = result.steps.last();
    out.extend_from_slice(format!(
        "],\"input_rows\":{},\"output_rows\":{},\"final_clock_micros\":{},\"final_watermark_micros\":{},\"eof\":{},\"future_dropped\":{},\"time\":{{\"processing_time\":{processing_time},\"event_time\":{event_time},\"clock\":\"virtual\",\"start_micros\":{}}},\"complete\":true,\"execution\":\"preview_virtual_clock\",\"side_effects\":false,\"ignored_fields\":{},\"not_verified\":{}}}",
        result.input_rows, result.output_rows, last.map_or(req.start_micros, |s| s.clock_micros), last.map_or(-1, |s| s.watermark_micros),
        result.eof, result.future_dropped, req.start_micros,
        serde_json::to_string(&ignored).unwrap_or_default(), serde_json::to_string(NOT_VERIFIED).unwrap_or_default()).as_bytes());
    Ok(out)
}

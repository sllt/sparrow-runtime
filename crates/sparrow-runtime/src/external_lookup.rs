//! Bounded asynchronous enrichment. Providers perform one read-only request,
//! never detach background work, and bound their encoding/frame/decoding memory
//! by `scratch_bytes`. The operator reserves that credit BEFORE calling them.
//! This is restart-fresh only: caches/external answers are not replay journals.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sparrow_model::{
    CreditKind, DataType, ErrorCode, MemoryLease, MemoryOwner, Result, Row, RowBatch,
    RowBatchBuilder, Scalar, Schema, SparrowError,
};
use sparrow_plan::LookupSpec;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::lookup::{
    encode_scalars, lookup_output_schema, scalar_key_len, schema_resident_bytes,
    validate_key_schema, validate_key_values, validate_row,
};
use crate::window::resolve_keys;

pub const EXTERNAL_LOOKUP_MAX_ROW_BYTES: usize = 64 * 1024;
const MAX_CACHE_ENTRIES: usize = 1024;
const MAX_PROVIDER_SCRATCH: usize = 2 * 1024 * 1024;

pub trait ExternalLookup: Send + Sync {
    /// Descriptor/keys/scratch remain immutable for the binding lifetime.
    fn schema(&self) -> &Schema;
    fn keys(&self) -> &[String];

    /// Application buffer upper bound for provider-local frame/encoding/
    /// decoding and its returned Row (not transport/TLS or total RSS). No
    /// request is invoked until this credit is acquired. The
    /// provider must independently enforce a 64 KiB wire and resident Row cap.
    fn scratch_bytes(&self) -> usize {
        512 * 1024
    }

    /// Dropping this future or cancelling `cancel` MUST stop request I/O. A
    /// provider must not spawn detached work. Transport failures use JobFailed;
    /// schema/codec/policy/limit errors are hard failures, never NULL fallback.
    fn lookup<'a>(
        &'a self,
        key: Vec<Scalar>,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + 'a>>;
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LookupErrorPolicy {
    #[default]
    Fail,
    Null,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ExternalLookupOptions {
    pub max_inflight: usize,
    pub timeout_ms: u64,
    pub cache_ttl_ms: u64,
    pub cache_bytes: usize,
    pub on_error: LookupErrorPolicy,
}

impl Default for ExternalLookupOptions {
    fn default() -> Self {
        Self {
            max_inflight: 4,
            timeout_ms: 1000,
            cache_ttl_ms: 1000,
            cache_bytes: 64 * 1024,
            on_error: LookupErrorPolicy::Fail,
        }
    }
}

impl ExternalLookupOptions {
    pub fn validate(&self) -> Result<()> {
        if !(1..=16).contains(&self.max_inflight)
            || !(10..=5000).contains(&self.timeout_ms)
            || self.cache_ttl_ms > 60_000
            || self.cache_bytes > 1024 * 1024
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "invalid bounded external lookup options",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ExternalLookupBinding {
    pub provider: Arc<dyn ExternalLookup>,
    pub options: ExternalLookupOptions,
}

impl std::fmt::Debug for ExternalLookupBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalLookupBinding")
            .field("options", &self.options)
            .field("schema", self.provider.schema())
            .field("keys", &self.provider.keys())
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct LookupDiagnostics {
    requests: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    null_keys: AtomicU64,
    cache_hits: AtomicU64,
    negative_cache_hits: AtomicU64,
    timeouts: AtomicU64,
    failures: AtomicU64,
    error_nulls: AtomicU64,
    cancellations: AtomicU64,
    inflight: AtomicUsize,
    peak_inflight: AtomicUsize,
    cache_entries: AtomicUsize,
    cache_bytes: AtomicUsize,
    // Observer Arcs can survive a completed JobHandle. Keep their already-
    // admitted metadata credit until the final diagnostic observer drops.
    _metadata: Option<Arc<MemoryLease>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LookupDiagnosticsSnapshot {
    pub requests: u64,
    pub hits: u64,
    pub misses: u64,
    pub null_keys: u64,
    pub cache_hits: u64,
    pub negative_cache_hits: u64,
    pub timeouts: u64,
    pub failures: u64,
    pub error_nulls: u64,
    pub cancellations: u64,
    pub inflight: usize,
    pub peak_inflight: usize,
    pub cache_entries: usize,
    pub cache_bytes: usize,
}

impl LookupDiagnostics {
    pub(crate) fn owned(metadata: Arc<MemoryLease>) -> Self {
        Self {
            _metadata: Some(metadata),
            ..Self::default()
        }
    }

    pub fn snapshot(&self) -> LookupDiagnosticsSnapshot {
        LookupDiagnosticsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            null_keys: self.null_keys.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            negative_cache_hits: self.negative_cache_hits.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            error_nulls: self.error_nulls.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            inflight: self.inflight.load(Ordering::Relaxed),
            peak_inflight: self.peak_inflight.load(Ordering::Relaxed),
            cache_entries: self.cache_entries.load(Ordering::Relaxed),
            cache_bytes: self.cache_bytes.load(Ordering::Relaxed),
        }
    }
}

struct RequestGauge(Arc<LookupDiagnostics>);
impl RequestGauge {
    fn new(diag: Arc<LookupDiagnostics>) -> Self {
        let now = diag.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        diag.peak_inflight.fetch_max(now, Ordering::Relaxed);
        diag.requests.fetch_add(1, Ordering::Relaxed);
        Self(diag)
    }
}
impl Drop for RequestGauge {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

struct CacheEntry {
    key: Vec<u8>,
    row: Option<Row>,
    expires: Instant,
    bytes: usize,
}

/// A preallocated bounded entry vector, rather than an unaccounted growing
/// hash table. Payload + slot capacity are both included in `cache_bytes`.
struct Cache {
    entries: Vec<CacheEntry>,
    used: usize,
    max_bytes: usize,
    ttl: Duration,
    diag: Arc<LookupDiagnostics>,
    _lease: Option<MemoryLease>,
}

impl Cache {
    fn new(
        options: &ExternalLookupOptions,
        owner: &Arc<MemoryOwner>,
        diag: Arc<LookupDiagnostics>,
    ) -> Result<Self> {
        let enabled = options.cache_bytes >= 256 && options.cache_ttl_ms > 0;
        let cap = if enabled {
            (options.cache_bytes / 256).min(MAX_CACHE_ENTRIES)
        } else {
            0
        };
        let lease = if enabled {
            Some(owner.acquire(CreditKind::Retention, options.cache_bytes)?)
        } else {
            None
        };
        let entries = Vec::with_capacity(cap);
        let used = cap.saturating_mul(std::mem::size_of::<CacheEntry>());
        diag.cache_bytes.fetch_add(used, Ordering::Relaxed);
        Ok(Self {
            entries,
            used,
            max_bytes: if enabled { options.cache_bytes } else { 0 },
            ttl: Duration::from_millis(options.cache_ttl_ms),
            diag,
            _lease: lease,
        })
    }

    fn remove(&mut self, index: usize) {
        let old = self.entries.remove(index);
        self.used -= old.bytes;
        self.diag.cache_entries.fetch_sub(1, Ordering::Relaxed);
        self.diag
            .cache_bytes
            .fetch_sub(old.bytes, Ordering::Relaxed);
    }

    fn expire(&mut self, now: Instant) {
        let mut i = 0;
        while i < self.entries.len() {
            if self.entries[i].expires <= now {
                self.remove(i);
            } else {
                i += 1;
            }
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Option<Row>> {
        self.expire(Instant::now());
        let entry = self.entries.iter().find(|entry| entry.key == key)?;
        self.diag.cache_hits.fetch_add(1, Ordering::Relaxed);
        if entry.row.is_none() {
            self.diag
                .negative_cache_hits
                .fetch_add(1, Ordering::Relaxed);
        }
        // Caller holds a window scratch lease before this detached copy.
        Some(entry.row.as_ref().map(Row::detach_copy))
    }

    fn insert(&mut self, key: Vec<u8>, row: &Option<Row>) {
        if self.entries.capacity() == 0 {
            return;
        }
        self.expire(Instant::now());
        if let Some(i) = self.entries.iter().position(|entry| entry.key == key) {
            self.remove(i);
        }
        // Include copying overlap and allocator/Arc overhead conservatively.
        let bytes = key
            .capacity()
            .saturating_add(row.as_ref().map_or(0, Row::resident_bytes))
            .saturating_add(128);
        let base = self
            .entries
            .capacity()
            .saturating_mul(std::mem::size_of::<CacheEntry>());
        if bytes > self.max_bytes.saturating_sub(base) {
            return;
        }
        while self.entries.len() == self.entries.capacity()
            || self.used.saturating_add(bytes) > self.max_bytes
        {
            self.remove(0);
        }
        self.entries.push(CacheEntry {
            key,
            row: row.as_ref().map(Row::detach_copy),
            expires: Instant::now() + self.ttl,
            bytes,
        });
        self.used += bytes;
        self.diag.cache_entries.fetch_add(1, Ordering::Relaxed);
        self.diag.cache_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}
impl Drop for Cache {
    fn drop(&mut self) {
        self.diag
            .cache_entries
            .fetch_sub(self.entries.len(), Ordering::Relaxed);
        self.diag
            .cache_bytes
            .fetch_sub(self.used, Ordering::Relaxed);
    }
}

pub struct ExternalLookupOperator {
    binding: ExternalLookupBinding,
    input: Schema,
    output: Schema,
    table: Schema,
    stream_idx: Vec<usize>,
    table_idx: Vec<usize>,
    keep_idx: Vec<usize>,
    owner: Arc<MemoryOwner>,
    cache: Cache,
    diagnostics: Arc<LookupDiagnostics>,
    _metadata: MemoryLease,
}

impl ExternalLookupOperator {
    pub fn new(
        spec: LookupSpec,
        binding: ExternalLookupBinding,
        input: Schema,
        owner: Arc<MemoryOwner>,
    ) -> Result<Self> {
        Self::with_diagnostics(
            spec,
            binding,
            input,
            owner,
            Arc::new(LookupDiagnostics::default()),
        )
    }

    pub fn with_diagnostics(
        spec: LookupSpec,
        binding: ExternalLookupBinding,
        input: Schema,
        owner: Arc<MemoryOwner>,
        diagnostics: Arc<LookupDiagnostics>,
    ) -> Result<Self> {
        let (stream_idx, table_idx, keep_idx, output) =
            Self::validate_binding(&spec, &binding, &input)?;
        let metadata_bytes = schema_resident_bytes(&input)
            .saturating_add(schema_resident_bytes(&output))
            .saturating_add(schema_resident_bytes(binding.provider.schema()))
            .saturating_add(1024);
        let metadata = owner.acquire(CreditKind::Retention, metadata_bytes)?;
        let table = binding.provider.schema().clone();
        let cache = Cache::new(&binding.options, &owner, Arc::clone(&diagnostics))?;
        Ok(Self {
            binding,
            input,
            output,
            table,
            stream_idx,
            table_idx,
            keep_idx,
            owner,
            cache,
            diagnostics,
            _metadata: metadata,
        })
    }

    /// Pure structural admission: no provider request, cache allocation or I/O.
    pub fn validate_binding(
        spec: &LookupSpec,
        binding: &ExternalLookupBinding,
        input: &Schema,
    ) -> Result<(Vec<usize>, Vec<usize>, Vec<usize>, Schema)> {
        spec.validate()?;
        input.validate()?;
        binding.options.validate()?;
        if spec.temporal || spec.as_of_field.is_some() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "external lookup is non-temporal and restart_fresh only",
            ));
        }
        let table = binding.provider.schema();
        table.validate()?;
        if table.fields.is_empty()
            || table.fields.len() > 16
            || binding.provider.keys().len() > 8
            || schema_resident_bytes(table) > 16 * 1024
            || table.fields.iter().any(|field| field.name.len() > 128)
            || table
                .fields
                .iter()
                .any(|field| !crate::lookup::deterministic_key_type(&field.data_type))
            || !(EXTERNAL_LOOKUP_MAX_ROW_BYTES..=MAX_PROVIDER_SCRATCH)
                .contains(&binding.provider.scratch_bytes())
        {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "external lookup requires bounded scratch and 1..16 scalar columns/1..8 keys",
            ));
        }
        if spec.table_keys != binding.provider.keys() {
            return Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                "external lookup table_keys must match provider key order",
            ));
        }
        let stream_idx = resolve_keys(input, &spec.stream_keys)?;
        let table_idx = resolve_keys(table, &spec.table_keys)?;
        validate_key_schema(table, &spec.table_keys, &table_idx)?;
        if table_idx
            .iter()
            .any(|&i| table.fields[i].data_type == DataType::Float64)
        {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "external lookup does not admit Float64 keys",
            ));
        }
        for (&stream, &table_column) in stream_idx.iter().zip(&table_idx) {
            if input.fields[stream].data_type != table.fields[table_column].data_type {
                return Err(SparrowError::new(
                    ErrorCode::TypeMismatch,
                    "external lookup input and provider key types differ",
                ));
            }
        }
        let keep: Vec<String> = if spec.keep.is_empty() {
            table
                .fields
                .iter()
                .filter(|field| !spec.table_keys.contains(&field.name))
                .map(|field| field.name.clone())
                .collect()
        } else {
            spec.keep.clone()
        };
        let keep_idx = resolve_keys(table, &keep)?;
        let output = lookup_output_schema(input, table, &keep)?;
        Ok((stream_idx, table_idx, keep_idx, output))
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output
    }
    pub fn diagnostics(&self) -> Arc<LookupDiagnostics> {
        Arc::clone(&self.diagnostics)
    }

    fn validate_input(&self, batch: &RowBatch) -> Result<()> {
        if batch.schema() != &self.input {
            return Err(SparrowError::new(
                ErrorCode::InvalidSchema,
                "external lookup batch schema mismatch",
            ));
        }
        if !Arc::ptr_eq(batch.lease().owner(), &self.owner) {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "external lookup batch belongs to a different memory owner",
            ));
        }
        if batch.num_rows() > self.owner.budget().max_rows {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                "external lookup input batch exceeds row bound",
            ));
        }
        for (index, row) in batch.rows().iter().enumerate() {
            validate_row(&self.input, row, index)?;
            validate_key_values(&self.input, &self.stream_idx, row, index, true)?;
            let key_bytes = self.stream_idx.iter().fold(0usize, |n, &i| {
                n.saturating_add(row.values[i].resident_bytes())
                    .saturating_add(scalar_key_len(&row.values[i]))
                    .saturating_add(1)
            });
            if key_bytes > EXTERNAL_LOOKUP_MAX_ROW_BYTES {
                return Err(SparrowError::new(
                    ErrorCode::MaxRecordSize,
                    "external lookup key exceeds resident bound",
                ));
            }
        }
        Ok(())
    }

    fn validate_response(&self, key: &[Scalar], row: &Row) -> Result<()> {
        if row.resident_bytes() > EXTERNAL_LOOKUP_MAX_ROW_BYTES {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                "external lookup response exceeds resident bound",
            ));
        }
        if row
            .values
            .iter()
            .any(|value| matches!(value, Scalar::Dynamic(_)))
        {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "external lookup returned a Dynamic value outside its scalar-only contract",
            ));
        }
        validate_row(&self.table, row, 0)?;
        validate_key_values(&self.table, &self.table_idx, row, 0, false)?;
        // A wrong-key row is a hard protocol violation, never a cache hit or
        // transport failure that can be softened by on_error:null.
        let returned: Vec<Scalar> = self
            .table_idx
            .iter()
            .map(|&i| row.values[i].clone())
            .collect();
        if encode_scalars(key) != encode_scalars(&returned) {
            return Err(SparrowError::new(
                ErrorCode::CodecViolation,
                "external lookup returned a different typed key",
            ));
        }
        if row
            .values
            .iter()
            .any(|value| matches!(value, Scalar::Float64(v) if !v.is_finite()))
        {
            return Err(SparrowError::new(
                ErrorCode::TypeMismatch,
                "external lookup returned a non-finite scalar",
            ));
        }
        Ok(())
    }

    fn output_row(&self, input: &Row, response: Option<&Row>) -> Row {
        let mut values = Vec::with_capacity(input.values.len().saturating_add(self.keep_idx.len()));
        values.extend(input.values.iter().map(Scalar::detach_copy));
        for &i in &self.keep_idx {
            values.push(response.map_or(Scalar::Null, |row| row.values[i].detach_copy()));
        }
        Row { values }
    }

    /// The entire input batch is prevalidated; only one completed, validated
    /// output batch is published. At most max_inflight request futures exist,
    /// and reorder storage is bounded to that same window. Errors cancel and
    /// JOIN all tasks before any window lease can be refunded.
    pub async fn on_batch_into(
        &mut self,
        batch: &RowBatch,
        cancel: CancellationToken,
    ) -> Result<Option<RowBatch>> {
        self.validate_input(batch)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let output_scratch_bytes = batch.rows().iter().fold(256usize, |n, row| {
            n.saturating_add(row.resident_bytes().saturating_mul(2))
                .saturating_add(
                    self.keep_idx
                        .len()
                        .saturating_mul(std::mem::size_of::<Scalar>()),
                )
                .saturating_add(128)
        });
        let _output_scratch = self
            .owner
            .acquire(CreditKind::Reservation, output_scratch_bytes)?;
        let mut builder = RowBatchBuilder::new(
            Arc::new(self.output.clone()),
            Arc::clone(&self.owner),
            CreditKind::Reservation,
            batch.num_rows(),
            self.owner.budget().reservation_bytes,
        )?;
        for inputs in batch.rows().chunks(self.binding.options.max_inflight) {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            let key_scratch = inputs.iter().fold(0usize, |n, row| {
                self.stream_idx.iter().fold(n, |n, &i| {
                    n.saturating_add(row.values[i].resident_bytes().saturating_mul(2))
                        .saturating_add(scalar_key_len(&row.values[i]).saturating_mul(3))
                        .saturating_add(32)
                })
            });
            let window_bytes = self
                .binding
                .provider
                .scratch_bytes()
                .saturating_add(EXTERNAL_LOOKUP_MAX_ROW_BYTES)
                .saturating_add(1024)
                .saturating_mul(inputs.len())
                .saturating_add(key_scratch)
                .saturating_add(512);
            let window_scratch =
                Arc::new(self.owner.acquire(CreditKind::Reservation, window_bytes)?);
            let requests_cancel = cancel.child_token();
            let mut set = JoinSet::new();
            let mut keys = Vec::with_capacity(inputs.len());
            let mut responses: Vec<Option<Option<Row>>> = (0..inputs.len()).map(|_| None).collect();
            let mut cacheable = vec![false; inputs.len()];
            for (index, input) in inputs.iter().enumerate() {
                let key: Vec<Scalar> = self
                    .stream_idx
                    .iter()
                    .map(|&i| input.values[i].detach_copy())
                    .collect();
                let encoded = encode_scalars(&key);
                if key.iter().any(Scalar::is_null) {
                    self.diagnostics.null_keys.fetch_add(1, Ordering::Relaxed);
                    responses[index] = Some(None);
                } else if let Some(cached) = self.cache.get(&encoded) {
                    responses[index] = Some(cached);
                } else {
                    let provider = Arc::clone(&self.binding.provider);
                    let request_cancel = requests_cancel.child_token();
                    let timeout = Duration::from_millis(self.binding.options.timeout_ms);
                    let gauge = RequestGauge::new(Arc::clone(&self.diagnostics));
                    let diag = Arc::clone(&self.diagnostics);
                    let request_scratch = Arc::clone(&window_scratch);
                    let query_key = key.iter().map(Scalar::detach_copy).collect();
                    set.spawn(async move {
                        let _gauge = gauge;
                        let result = tokio::select! {
                            biased;
                            _ = request_cancel.cancelled() => {
                                diag.cancellations.fetch_add(1, Ordering::Relaxed);
                                Err(cancelled())
                            }
                            result = tokio::time::timeout(timeout, provider.lookup(query_key, request_cancel.clone())) => {
                                match result {
                                    Ok(result) => result,
                                    Err(_) => {
                                        request_cancel.cancel();
                                        diag.timeouts.fetch_add(1, Ordering::Relaxed);
                                        Err(SparrowError::new(ErrorCode::JobFailed, "external lookup request timed out"))
                                    }
                                }
                            }
                        };
                        // Also retain the lease in the completed result: if the
                        // stage itself is aborted, queued/unjoined responses
                        // cannot outlive their memory credits.
                        (index, result, request_scratch)
                    });
                }
                keys.push((key, encoded));
            }
            let mut failed = None;
            while !set.is_empty() {
                let next = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { failed = Some(cancelled()); break; }
                    next = set.join_next() => next,
                };
                match next {
                    Some(Ok((index, Ok(row), _scratch))) => {
                        if let Some(row) = &row {
                            if let Err(error) = self.validate_response(&keys[index].0, row) {
                                self.diagnostics.failures.fetch_add(1, Ordering::Relaxed);
                                failed = Some(error);
                                break;
                            }
                        }
                        cacheable[index] = true;
                        responses[index] = Some(row);
                    }
                    Some(Ok((index, Err(error), _scratch))) => {
                        self.diagnostics.failures.fetch_add(1, Ordering::Relaxed);
                        if error.code == ErrorCode::JobFailed
                            && self.binding.options.on_error == LookupErrorPolicy::Null
                        {
                            self.diagnostics.error_nulls.fetch_add(1, Ordering::Relaxed);
                            responses[index] = Some(None);
                        } else {
                            failed = Some(error);
                            break;
                        }
                    }
                    Some(Err(_)) => {
                        failed = Some(SparrowError::new(
                            ErrorCode::JobFailed,
                            "external lookup request task failed",
                        ));
                        break;
                    }
                    None => break,
                }
            }
            if failed.is_some() {
                requests_cancel.cancel();
                set.abort_all();
                while set.join_next().await.is_some() {}
                return Err(failed.expect("failure recorded"));
            }
            // All outstanding I/O has joined before validation/caching/output
            // conversion can fail. A control envelope cannot overtake this batch.
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            for (index, input) in inputs.iter().enumerate() {
                let response = responses[index].take().ok_or_else(|| {
                    SparrowError::new(
                        ErrorCode::Internal,
                        "external lookup missing ordered response",
                    )
                })?;
                if response.is_some() {
                    self.diagnostics.hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.diagnostics.misses.fetch_add(1, Ordering::Relaxed);
                }
                if cacheable[index] {
                    self.cache
                        .insert(std::mem::take(&mut keys[index].1), &response);
                }
                let output = self.output_row(input, response.as_ref());
                let bytes = output.resident_bytes().saturating_add(64);
                builder.push_accounted(output, bytes)?;
            }
        }
        if cancel.is_cancelled() {
            return Err(cancelled());
        }
        Ok(Some(builder.finish()?))
    }
}

fn cancelled() -> SparrowError {
    SparrowError::new(ErrorCode::Cancelled, "external lookup cancelled")
}

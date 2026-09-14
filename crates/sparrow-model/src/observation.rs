//! Bounded, process-monotonic diagnostics. Not persisted state or delivery IDs.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{CreditKind, ErrorCode, MemoryLease, MemoryOwner, Result};

pub fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

/// Bounds of *local* observation times, not device event time. Filtering may
/// retain wider input-batch bounds; aggregation must clear them unless proven.
#[derive(Clone, Copy, Debug, Default)]
pub struct OriginSpan {
    pub first: Option<Instant>,
    pub last: Option<Instant>,
    pub unknown: bool,
}
impl OriginSpan {
    pub fn at(at: Instant) -> Self {
        Self {
            first: Some(at),
            last: Some(at),
            unknown: false,
        }
    }
    pub fn missing() -> Self {
        Self {
            unknown: true,
            ..Self::default()
        }
    }
    pub fn merge(&mut self, other: Self) {
        self.unknown |= other.unknown;
        if let Some(at) = other.first {
            self.first = Some(self.first.map_or(at, |v| v.min(at)));
        }
        if let Some(at) = other.last {
            self.last = Some(self.last.map_or(at, |v| v.max(at)));
        }
    }
    pub fn oldest_age(self, now: Instant) -> Option<Duration> {
        if self.unknown {
            None
        } else {
            self.first.and_then(|at| now.checked_duration_since(at))
        }
    }
}

pub const HISTOGRAM_BUCKETS: usize = 32;
#[derive(Clone, Debug, Default)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum_us: u64,
    pub max_us: u64,
    pub buckets: [u64; HISTOGRAM_BUCKETS],
}
impl HistogramSnapshot {
    pub fn record(&mut self, duration: Duration) {
        let us = micros(duration);
        // Finite bucket upper bounds 1,2,4,...,2^30 us, then overflow.
        let bucket = if us <= 1 {
            0
        } else {
            (64 - (us - 1).leading_zeros() as usize).min(31)
        };
        self.count = self.count.saturating_add(1);
        self.sum_us = self.sum_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
    }
    /// Upper-bound estimate; missing/insufficient/overflow samples return None.
    pub fn percentile_upper_us(&self, percentile: u64) -> Option<u64> {
        if self.count < 100 || !(1..=100).contains(&percentile) {
            return None;
        }
        let rank = ((self.count as u128 * percentile as u128 + 99) / 100) as u64;
        let mut seen = 0u64;
        for (i, n) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*n);
            if seen >= rank {
                return (i < 31).then(|| 1u64 << i);
            }
        }
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthState {
    Initializing,
    Connecting,
    Ready,
    Reconnecting,
    WaitingForAppend,
    Eof,
    Failed,
    Stopped,
}
impl HealthState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Initializing => "initializing",
            Self::Connecting => "connecting",
            Self::Ready => "ready",
            Self::Reconnecting => "reconnecting",
            Self::WaitingForAppend => "waiting_for_append",
            Self::Eof => "eof",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}
#[derive(Clone, Debug)]
pub struct EndpointSnapshot {
    pub state: HealthState,
    pub reason: &'static str,
    pub last_error_code: Option<ErrorCode>,
    pub changed_age_us: u64,
    pub progress_age_us: Option<u64>,
    pub progress_units: u64,
    pub failures: u64,
}
struct Endpoint {
    state: HealthState,
    reason: &'static str,
    last_error: Option<ErrorCode>,
    changed: Instant,
    progress: Option<Instant>,
    units: u64,
    failures: u64,
}
impl Endpoint {
    fn new() -> Self {
        Self {
            state: HealthState::Initializing,
            reason: "not_started",
            last_error: None,
            changed: Instant::now(),
            progress: None,
            units: 0,
            failures: 0,
        }
    }
    fn snapshot(&self, now: Instant) -> EndpointSnapshot {
        EndpointSnapshot {
            state: self.state,
            reason: self.reason,
            last_error_code: self.last_error,
            changed_age_us: micros(now.saturating_duration_since(self.changed)),
            progress_age_us: self
                .progress
                .map(|v| micros(now.saturating_duration_since(v))),
            progress_units: self.units,
            failures: self.failures,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Latency {
    Decode,
    FileReadDecode,
    Transform,
    Window,
    Dedup,
    Lookup,
    Encode,
    HttpHeaders,
    HttpBody,
    SinkDelivery,
    LocalOriginToSink,
    SourceAdmission,
}
impl Latency {
    pub const ALL: [Self; 12] = [
        Self::Decode,
        Self::FileReadDecode,
        Self::Transform,
        Self::Window,
        Self::Dedup,
        Self::Lookup,
        Self::Encode,
        Self::HttpHeaders,
        Self::HttpBody,
        Self::SinkDelivery,
        Self::LocalOriginToSink,
        Self::SourceAdmission,
    ];
    pub fn name(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::FileReadDecode => "file_read_decode",
            Self::Transform => "transform",
            Self::Window => "window",
            Self::Dedup => "dedup",
            Self::Lookup => "lookup",
            Self::Encode => "encode",
            Self::HttpHeaders => "http_response_headers",
            Self::HttpBody => "http_response_body",
            Self::SinkDelivery => "sink_delivery",
            Self::LocalOriginToSink => "local_origin_to_sink",
            Self::SourceAdmission => "source_admission",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeliverySnapshot {
    pub runtime_ingested_rows: u64,
    pub runtime_emitted_rows: u64,
    pub transform_filtered_rows: u64,
    pub encoded_credit_bytes: u64,
    pub peak_encoded_credit_bytes: u64,
    pub active_http_requests: u64,
    pub http_attempts_started: u64,
    pub http_attempts_finished: u64,
    pub http_attempts_cancelled: u64,
    pub active_groups: u64,
    pub active_rows: u64,
    pub active_bytes: u64,
    pub peak_groups: u64,
    pub peak_bytes: u64,
    pub accepted_groups: u64,
    pub accepted_rows: u64,
    pub completed_groups: u64,
    pub completed_rows: u64,
    pub failed_groups: u64,
    pub failed_rows: u64,
    pub unknown_origin_groups: u64,
    pub body_incomplete: u64,
}
/// Separate cache lines for independently sampled progress from different stages.
#[repr(align(64))]
#[derive(Default)]
struct ProgressCounter(AtomicU64);
impl ProgressCounter {
    fn add(&self, rows: usize) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(rows as u64))
            });
    }
    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
struct FlowState {
    ingested: ProgressCounter,
    emitted: ProgressCounter,
    filtered: ProgressCounter,
    source: Mutex<Endpoint>,
    sink: Mutex<Endpoint>,
    histograms: [Mutex<HistogramSnapshot>; 12],
    delivery: Mutex<DeliverySnapshot>,
    _lease: MemoryLease,
}
/// Small uninitialized handle. The sizeable metrics are allocated only AFTER
/// Kernel admission acquires their credit; no global per-event registry.
#[derive(Default)]
pub struct FlowObservation {
    state: OnceLock<Arc<FlowState>>,
}
impl std::fmt::Debug for FlowObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowObservation")
            .field("initialized", &self.state.get().is_some())
            .finish()
    }
}
impl FlowObservation {
    pub fn runtime_rows(&self, ingest: bool, rows: usize) {
        if let Some(s) = self.state.get() {
            if ingest {
                s.ingested.add(rows);
            } else {
                s.emitted.add(rows);
            }
        }
    }
    pub fn filtered(&self, rows: usize) {
        if let Some(s) = self.state.get() {
            s.filtered.add(rows);
        }
    }
    pub fn encoded_credit(self: &Arc<Self>, bytes: usize) -> EncodedGuard {
        let mut guard = EncodedGuard {
            observation: self.clone(),
            bytes: 0,
        };
        guard.resize(bytes);
        guard
    }
    pub fn request(self: &Arc<Self>) -> RequestGuard {
        let tracked = self.state.get().is_some();
        if let Some(s) = self.state.get() {
            let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner());
            d.active_http_requests += 1;
            d.http_attempts_started += 1;
        }
        RequestGuard {
            observation: self.clone(),
            tracked,
            finished: false,
        }
    }
    pub fn lifecycle(self: &Arc<Self>, source: bool) -> EndpointGuard {
        EndpointGuard {
            observation: self.clone(),
            source,
        }
    }
    pub fn timer(self: &Arc<Self>, metric: Latency) -> LatencyGuard {
        LatencyGuard {
            observation: self.clone(),
            metric,
            started: Instant::now(),
        }
    }
    pub fn metadata_bytes() -> usize {
        std::mem::size_of::<FlowState>() + 512
    }
    pub fn initialize(&self, owner: &Arc<MemoryOwner>) -> Result<()> {
        if self.state.get().is_some() {
            return Ok(());
        }
        let lease = owner.acquire(CreditKind::Queue, Self::metadata_bytes())?;
        let _ = self.state.set(Arc::new(FlowState {
            ingested: ProgressCounter::default(),
            emitted: ProgressCounter::default(),
            filtered: ProgressCounter::default(),
            source: Mutex::new(Endpoint::new()),
            sink: Mutex::new(Endpoint::new()),
            histograms: std::array::from_fn(|_| Mutex::new(HistogramSnapshot::default())),
            delivery: Mutex::new(DeliverySnapshot::default()),
            _lease: lease,
        }));
        Ok(())
    }
    pub fn health(
        &self,
        source: bool,
        state: HealthState,
        reason: &'static str,
        error: Option<ErrorCode>,
    ) {
        if let Some(s) = self.state.get() {
            let mut e = if source { &s.source } else { &s.sink }
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if e.state != state || e.reason != reason {
                e.changed = Instant::now();
            }
            e.state = state;
            e.reason = reason;
            if error.is_some() {
                e.last_error = error;
                e.failures = e.failures.saturating_add(1);
            }
        }
    }
    pub fn progress(&self, source: bool, units: usize) {
        if let Some(s) = self.state.get() {
            let mut e = if source { &s.source } else { &s.sink }
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            e.progress = Some(Instant::now());
            e.units = e.units.saturating_add(units as u64);
        }
    }
    pub fn endpoints(&self) -> Option<(EndpointSnapshot, EndpointSnapshot)> {
        self.state.get().map(|s| {
            let now = Instant::now();
            (
                s.source
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .snapshot(now),
                s.sink
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .snapshot(now),
            )
        })
    }
    pub fn record(&self, metric: Latency, duration: Duration) {
        if let Some(s) = self.state.get() {
            s.histograms[metric as usize]
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .record(duration);
        }
    }
    pub fn histograms(&self) -> Vec<(&'static str, HistogramSnapshot)> {
        self.state
            .get()
            .map(|s| {
                Latency::ALL
                    .iter()
                    .map(|m| {
                        (
                            m.name(),
                            s.histograms[*m as usize]
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn delivery(&self) -> Option<DeliverySnapshot> {
        self.state.get().map(|s| {
            // Delivery conservation is still one coherent mutex snapshot.
            // Progress counters are independent monotonic samples, not a cut.
            let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner()).clone();
            d.runtime_ingested_rows = s.ingested.get();
            d.runtime_emitted_rows = s.emitted.get();
            d.transform_filtered_rows = s.filtered.get();
            d
        })
    }
    pub fn body_incomplete(&self) {
        if let Some(s) = self.state.get() {
            let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner());
            d.body_incomplete = d.body_incomplete.saturating_add(1);
        }
    }
    pub fn delivery_guard(
        self: &Arc<Self>,
        rows: usize,
        bytes: usize,
        origin: OriginSpan,
    ) -> DeliveryGuard {
        if let Some(s) = self.state.get() {
            let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner());
            d.accepted_groups += 1;
            d.accepted_rows += rows as u64;
            d.active_groups += 1;
            d.active_rows += rows as u64;
            d.active_bytes += bytes as u64;
            d.peak_groups = d.peak_groups.max(d.active_groups);
            d.peak_bytes = d.peak_bytes.max(d.active_bytes);
        }
        DeliveryGuard {
            observation: self.clone(),
            rows,
            bytes,
            groups: 1,
            origin,
            started: Instant::now(),
            ok: false,
        }
    }
}
pub struct RequestGuard {
    observation: Arc<FlowObservation>,
    tracked: bool,
    finished: bool,
}
impl RequestGuard {
    pub fn finish(&mut self) {
        self.finished = true;
    }
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        if self.tracked {
            if let Some(s) = self.observation.state.get() {
                let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner());
                d.active_http_requests -= 1;
                if self.finished {
                    d.http_attempts_finished += 1;
                } else {
                    d.http_attempts_cancelled += 1;
                }
            }
        }
    }
}
pub struct EncodedGuard {
    observation: Arc<FlowObservation>,
    bytes: usize,
}
impl EncodedGuard {
    pub fn resize(&mut self, bytes: usize) {
        if let Some(s) = self.observation.state.get() {
            let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner());
            d.encoded_credit_bytes = d.encoded_credit_bytes - self.bytes as u64 + bytes as u64;
            d.peak_encoded_credit_bytes = d.peak_encoded_credit_bytes.max(d.encoded_credit_bytes);
            self.bytes = bytes;
        }
    }
}
impl Drop for EncodedGuard {
    fn drop(&mut self) {
        self.resize(0);
    }
}
pub struct LatencyGuard {
    observation: Arc<FlowObservation>,
    metric: Latency,
    started: Instant,
}
impl Drop for LatencyGuard {
    fn drop(&mut self) {
        self.observation.record(self.metric, self.started.elapsed());
    }
}
pub struct EndpointGuard {
    observation: Arc<FlowObservation>,
    source: bool,
}
impl Drop for EndpointGuard {
    fn drop(&mut self) {
        if let Some(s) = self.observation.state.get() {
            let mut e = if self.source { &s.source } else { &s.sink }
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !matches!(e.state, HealthState::Eof | HealthState::Failed) {
                e.state = HealthState::Stopped;
                e.reason = "task_ended";
                e.changed = Instant::now();
            }
        }
    }
}
/// One dequeued upstream batch. Moves through HTTP coalescing; drop/cancel is
/// failed, not acknowledged. No payload retained by observations.
pub struct DeliveryGuard {
    observation: Arc<FlowObservation>,
    rows: usize,
    bytes: usize,
    groups: u64,
    origin: OriginSpan,
    started: Instant,
    ok: bool,
}
impl DeliveryGuard {
    pub fn complete(&mut self) {
        self.ok = true;
    }
    /// Constant-size coalescing, no per-input guard vector or payload ownership.
    pub fn merge(&mut self, mut other: Self) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.groups += other.groups;
        self.origin.merge(other.origin);
        self.started = self.started.min(other.started);
        other.groups = 0;
    }
}
impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.groups == 0 {
            return;
        }
        if let Some(s) = self.observation.state.get() {
            let now = Instant::now();
            let mut d = s.delivery.lock().unwrap_or_else(|e| e.into_inner());
            d.active_groups -= self.groups;
            d.active_rows -= self.rows as u64;
            d.active_bytes -= self.bytes as u64;
            if self.ok {
                d.completed_groups += self.groups;
                d.completed_rows += self.rows as u64;
            } else {
                d.failed_groups += self.groups;
                d.failed_rows += self.rows as u64;
            }
            if self.origin.oldest_age(now).is_none() {
                d.unknown_origin_groups += self.groups;
            }
            drop(d);
            self.observation.record(
                Latency::SinkDelivery,
                now.saturating_duration_since(self.started),
            );
            if let Some(age) = self.origin.oldest_age(now) {
                self.observation.record(Latency::LocalOriginToSink, age);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn r9_progress_counters_are_independent_saturating_and_owner_bounded() {
        let owner = MemoryOwner::new(crate::ResourceBudget::compact());
        let obs = Arc::new(FlowObservation::default());
        obs.initialize(&owner).unwrap();
        std::thread::scope(|scope| {
            for stage in 0..3 {
                let obs = &obs;
                scope.spawn(move || {
                    for _ in 0..10000 {
                        match stage {
                            0 => obs.runtime_rows(true, 8),
                            1 => obs.runtime_rows(false, 2),
                            _ => obs.filtered(6),
                        }
                    }
                });
            }
        });
        let d = obs.delivery().unwrap();
        assert_eq!(
            (
                d.runtime_ingested_rows,
                d.runtime_emitted_rows,
                d.transform_filtered_rows
            ),
            (80000, 20000, 60000)
        );
        assert_eq!(
            (
                d.accepted_groups,
                d.completed_groups,
                d.failed_groups,
                d.active_groups
            ),
            (0, 0, 0, 0)
        );
        let counter = ProgressCounter(AtomicU64::new(u64::MAX - 1));
        counter.add(8);
        assert_eq!(counter.get(), u64::MAX);
        drop(obs);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
    #[test]
    fn obs_histogram_bounds_unknown_and_overflow() {
        let mut h = HistogramSnapshot::default();
        assert_eq!(h.percentile_upper_us(99), None);
        for _ in 0..100 {
            h.record(Duration::from_micros(3));
        }
        assert_eq!(h.percentile_upper_us(99), Some(4));
        assert_eq!(h.buckets.iter().sum::<u64>(), 100);
        for _ in 0..10000 {
            h.record(Duration::from_secs(u32::MAX as u64));
        }
        assert_eq!(h.percentile_upper_us(99), None);
    }
    #[test]
    fn obs_origin_merge_never_turns_unknown_into_known() {
        let now = Instant::now();
        let mut s = OriginSpan::at(now);
        s.merge(OriginSpan::missing());
        assert!(s.oldest_age(now).is_none());
        assert!(OriginSpan::at(now + Duration::from_secs(1))
            .oldest_age(now)
            .is_none());
    }
    #[test]
    fn obs_guards_close_failed_work_and_metadata_credit() {
        let owner = MemoryOwner::new(crate::ResourceBudget::compact());
        let obs = Arc::new(FlowObservation::default());
        obs.initialize(&owner).unwrap();
        {
            let mut a = obs.delivery_guard(2, 40, OriginSpan::at(Instant::now()));
            let _b = obs.delivery_guard(1, 20, OriginSpan::missing());
            a.complete();
        }
        let d = obs.delivery().unwrap();
        assert_eq!((d.active_groups, d.active_bytes), (0, 0));
        assert_eq!(
            (d.completed_rows, d.failed_rows, d.unknown_origin_groups),
            (2, 1, 1)
        );
        assert!(owner.usage().queue_bytes > 0);
        drop(obs);
        assert_eq!(owner.usage().physical_bytes, 0);
    }
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Process-wide I/O counters for ack/diagnostics. Live path only — not a
/// checkpoint or delivery receipt.
#[derive(Debug, Default)]
pub struct IoDiagnostics {
    pub mqtt_received: AtomicU64,
    pub mqtt_inbox: Arc<sparrow_model::QueueOccupancy>,
    pub mqtt_inbox_metadata_bytes: AtomicU64,
    pub mqtt_pending_bytes: AtomicU64,
    pub mqtt_dropped_budget: AtomicU64,
    pub mqtt_dropped_oversize: AtomicU64,
    pub mqtt_accounted_sources: AtomicU64,
    pub mqtt_decoded: AtomicU64,
    pub mqtt_dropped_bad: AtomicU64,
    pub mqtt_dropped_full: AtomicU64,
    pub mqtt_backpressure_waits: AtomicU64,
    pub mqtt_backpressure_recovered: AtomicU64,
    pub mqtt_reconnects: AtomicU64,
    pub mqtt_quickack_calls: AtomicU64,
    pub mqtt_quickack_errors: AtomicU64,
    pub http_posted: AtomicU64,
    pub http_acked_batches: AtomicU64,
    pub http_encode_errors: AtomicU64,
    pub http_budget_drops: AtomicU64,
    pub http_failed: AtomicU64,
    pub http_dropped: AtomicU64,
    pub http_retries: AtomicU64,
    pub http_inflight: AtomicU64,
    pub log_written: AtomicU64,
    pub decode_errors: AtomicU64,
}

impl IoDiagnostics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            mqtt_received: self.mqtt_received.load(Ordering::Relaxed),
            mqtt_inbox_metadata_bytes: self.mqtt_inbox_metadata_bytes.load(Ordering::Relaxed),
            mqtt_pending_bytes: self.mqtt_pending_bytes.load(Ordering::Relaxed),
            mqtt_dropped_budget: self.mqtt_dropped_budget.load(Ordering::Relaxed),
            mqtt_dropped_oversize: self.mqtt_dropped_oversize.load(Ordering::Relaxed),
            mqtt_accounted_sources: self.mqtt_accounted_sources.load(Ordering::Relaxed),
            mqtt_inbox_items: self.mqtt_inbox.items.load(Ordering::Relaxed),
            mqtt_inbox_bytes: self.mqtt_inbox.bytes.load(Ordering::Relaxed),
            mqtt_inbox_peak_items: self.mqtt_inbox.peak_items.load(Ordering::Relaxed),
            mqtt_inbox_peak_bytes: self.mqtt_inbox.peak_bytes.load(Ordering::Relaxed),
            mqtt_decoded: self.mqtt_decoded.load(Ordering::Relaxed),
            mqtt_dropped_bad: self.mqtt_dropped_bad.load(Ordering::Relaxed),
            mqtt_dropped_full: self.mqtt_dropped_full.load(Ordering::Relaxed),
            mqtt_backpressure_waits: self.mqtt_backpressure_waits.load(Ordering::Relaxed),
            mqtt_backpressure_recovered: self.mqtt_backpressure_recovered.load(Ordering::Relaxed),
            mqtt_reconnects: self.mqtt_reconnects.load(Ordering::Relaxed),
            mqtt_quickack_calls: self.mqtt_quickack_calls.load(Ordering::Relaxed),
            mqtt_quickack_errors: self.mqtt_quickack_errors.load(Ordering::Relaxed),
            http_posted: self.http_posted.load(Ordering::Relaxed),
            http_acked_batches: self.http_acked_batches.load(Ordering::Relaxed),
            http_encode_errors: self.http_encode_errors.load(Ordering::Relaxed),
            http_budget_drops: self.http_budget_drops.load(Ordering::Relaxed),
            http_failed: self.http_failed.load(Ordering::Relaxed),
            http_dropped: self.http_dropped.load(Ordering::Relaxed),
            http_retries: self.http_retries.load(Ordering::Relaxed),
            http_inflight: self.http_inflight.load(Ordering::Relaxed),
            log_written: self.log_written.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IoSnapshot {
    pub mqtt_received: u64,
    pub mqtt_inbox_metadata_bytes: u64,
    pub mqtt_pending_bytes: u64,
    pub mqtt_dropped_budget: u64,
    pub mqtt_dropped_oversize: u64,
    pub mqtt_accounted_sources: u64,
    pub mqtt_inbox_items: u64,
    pub mqtt_inbox_bytes: u64,
    pub mqtt_inbox_peak_items: u64,
    pub mqtt_inbox_peak_bytes: u64,
    pub mqtt_decoded: u64,
    pub mqtt_dropped_bad: u64,
    pub mqtt_dropped_full: u64,
    pub mqtt_backpressure_waits: u64,
    pub mqtt_backpressure_recovered: u64,
    pub mqtt_reconnects: u64,
    pub mqtt_quickack_calls: u64,
    pub mqtt_quickack_errors: u64,
    pub http_posted: u64,
    pub http_acked_batches: u64,
    pub http_encode_errors: u64,
    pub http_budget_drops: u64,
    pub http_failed: u64,
    pub http_dropped: u64,
    pub http_retries: u64,
    pub http_inflight: u64,
    pub log_written: u64,
    pub decode_errors: u64,
}

impl std::fmt::Display for IoSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mqtt recv={} decoded={} bad={} full={} waits={} recovered={} reconnects={} | http posted={} failed={} dropped={} retries={} inflight={} | log={} decode_err={}",
            self.mqtt_received,
            self.mqtt_decoded,
            self.mqtt_dropped_bad,
            self.mqtt_dropped_full,
            self.mqtt_backpressure_waits,
            self.mqtt_backpressure_recovered,
            self.mqtt_reconnects,
            self.http_posted,
            self.http_failed,
            self.http_dropped,
            self.http_retries,
            self.http_inflight,
            self.log_written,
            self.decode_errors
        )
    }
}

impl IoSnapshot {
    pub fn add_assign(&mut self, other: &IoSnapshot) {
        self.mqtt_received += other.mqtt_received;
        self.mqtt_inbox_metadata_bytes += other.mqtt_inbox_metadata_bytes;
        self.mqtt_pending_bytes += other.mqtt_pending_bytes;
        self.mqtt_dropped_budget += other.mqtt_dropped_budget;
        self.mqtt_dropped_oversize += other.mqtt_dropped_oversize;
        self.mqtt_accounted_sources += other.mqtt_accounted_sources;
        self.mqtt_inbox_items += other.mqtt_inbox_items;
        self.mqtt_inbox_bytes += other.mqtt_inbox_bytes;
        self.mqtt_inbox_peak_items += other.mqtt_inbox_peak_items;
        self.mqtt_inbox_peak_bytes += other.mqtt_inbox_peak_bytes;
        self.mqtt_decoded += other.mqtt_decoded;
        self.mqtt_dropped_bad += other.mqtt_dropped_bad;
        self.mqtt_dropped_full += other.mqtt_dropped_full;
        self.mqtt_backpressure_waits += other.mqtt_backpressure_waits;
        self.mqtt_backpressure_recovered += other.mqtt_backpressure_recovered;
        self.mqtt_reconnects += other.mqtt_reconnects;
        self.mqtt_quickack_calls += other.mqtt_quickack_calls;
        self.mqtt_quickack_errors += other.mqtt_quickack_errors;
        self.http_posted += other.http_posted;
        self.http_acked_batches += other.http_acked_batches;
        self.http_encode_errors += other.http_encode_errors;
        self.http_budget_drops += other.http_budget_drops;
        self.http_failed += other.http_failed;
        self.http_dropped += other.http_dropped;
        self.http_retries += other.http_retries;
        self.http_inflight += other.http_inflight;
        self.log_written += other.log_written;
        self.decode_errors += other.decode_errors;
    }
}

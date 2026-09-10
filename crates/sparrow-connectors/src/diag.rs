use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Process-wide I/O counters for ack/diagnostics. Live path only — not a
/// checkpoint or delivery receipt.
#[derive(Debug, Default)]
pub struct IoDiagnostics {
    pub mqtt_received: AtomicU64,
    pub mqtt_decoded: AtomicU64,
    pub mqtt_dropped_bad: AtomicU64,
    pub mqtt_dropped_full: AtomicU64,
    pub mqtt_reconnects: AtomicU64,
    pub http_posted: AtomicU64,
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
            mqtt_decoded: self.mqtt_decoded.load(Ordering::Relaxed),
            mqtt_dropped_bad: self.mqtt_dropped_bad.load(Ordering::Relaxed),
            mqtt_dropped_full: self.mqtt_dropped_full.load(Ordering::Relaxed),
            mqtt_reconnects: self.mqtt_reconnects.load(Ordering::Relaxed),
            http_posted: self.http_posted.load(Ordering::Relaxed),
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
    pub mqtt_decoded: u64,
    pub mqtt_dropped_bad: u64,
    pub mqtt_dropped_full: u64,
    pub mqtt_reconnects: u64,
    pub http_posted: u64,
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
            "mqtt recv={} decoded={} bad={} full={} reconnects={} | http posted={} failed={} dropped={} retries={} inflight={} | log={} decode_err={}",
            self.mqtt_received,
            self.mqtt_decoded,
            self.mqtt_dropped_bad,
            self.mqtt_dropped_full,
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
        self.mqtt_decoded += other.mqtt_decoded;
        self.mqtt_dropped_bad += other.mqtt_dropped_bad;
        self.mqtt_dropped_full += other.mqtt_dropped_full;
        self.mqtt_reconnects += other.mqtt_reconnects;
        self.http_posted += other.http_posted;
        self.http_failed += other.http_failed;
        self.http_dropped += other.http_dropped;
        self.http_retries += other.http_retries;
        self.http_inflight += other.http_inflight;
        self.log_written += other.log_written;
        self.decode_errors += other.decode_errors;
    }
}

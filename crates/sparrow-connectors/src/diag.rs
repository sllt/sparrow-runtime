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
}

impl std::fmt::Display for IoSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mqtt recv={} decoded={} bad={} full={} reconnects={} | http posted={} failed={} dropped={} retries={} inflight={} | log={}",
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
            self.log_written
        )
    }
}

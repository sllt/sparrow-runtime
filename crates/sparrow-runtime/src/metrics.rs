//! Budgeted runtime counters. No per-event / per-key unbounded labels.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Process-wide (or session-wide) counters for V1 observability.
#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    pub jobs_started: AtomicU64,
    pub jobs_stopped: AtomicU64,
    pub ingested_rows: AtomicU64,
    pub emitted_rows: AtomicU64,
    pub queue_items: AtomicU64,
    pub queue_bytes: AtomicU64,
    pub watermark_lag_micros: AtomicI64,
    pub checkpoint_duration_micros: AtomicU64,
    pub checkpoint_bytes: AtomicU64,
    pub checkpoint_commits: AtomicU64,
    pub checkpoint_aborts: AtomicU64,
}

impl RuntimeMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_ingest(&self, n: u64) {
        self.ingested_rows.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_emit(&self, n: u64) {
        self.emitted_rows.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_queue(&self, items: u64, bytes: u64) {
        self.queue_items.store(items, Ordering::Relaxed);
        self.queue_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn record_watermark_lag(&self, lag_micros: i64) {
        self.watermark_lag_micros
            .store(lag_micros, Ordering::Relaxed);
    }

    pub fn record_checkpoint(&self, duration: Duration, bytes: u64) {
        self.checkpoint_duration_micros
            .store(duration.as_micros() as u64, Ordering::Relaxed);
        self.checkpoint_bytes.store(bytes, Ordering::Relaxed);
        self.checkpoint_commits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checkpoint_abort(&self) {
        self.checkpoint_aborts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            jobs_started: self.jobs_started.load(Ordering::Relaxed),
            jobs_stopped: self.jobs_stopped.load(Ordering::Relaxed),
            ingested_rows: self.ingested_rows.load(Ordering::Relaxed),
            emitted_rows: self.emitted_rows.load(Ordering::Relaxed),
            queue_items: self.queue_items.load(Ordering::Relaxed),
            queue_bytes: self.queue_bytes.load(Ordering::Relaxed),
            watermark_lag_micros: self.watermark_lag_micros.load(Ordering::Relaxed),
            checkpoint_duration_micros: self.checkpoint_duration_micros.load(Ordering::Relaxed),
            checkpoint_bytes: self.checkpoint_bytes.load(Ordering::Relaxed),
            checkpoint_commits: self.checkpoint_commits.load(Ordering::Relaxed),
            checkpoint_aborts: self.checkpoint_aborts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub jobs_started: u64,
    pub jobs_stopped: u64,
    pub ingested_rows: u64,
    pub emitted_rows: u64,
    pub queue_items: u64,
    pub queue_bytes: u64,
    pub watermark_lag_micros: i64,
    pub checkpoint_duration_micros: u64,
    pub checkpoint_bytes: u64,
    pub checkpoint_commits: u64,
    pub checkpoint_aborts: u64,
}

impl MetricsSnapshot {
    /// Structured log line (budgeted labels only).
    pub fn log_line(&self) -> String {
        format!(
            "{{\"event\":\"sparrow_metrics\",\"jobs_started\":{},\"jobs_stopped\":{},\"ingested_rows\":{},\"emitted_rows\":{},\"queue_items\":{},\"queue_bytes\":{},\"watermark_lag_micros\":{},\"checkpoint_duration_micros\":{},\"checkpoint_bytes\":{},\"checkpoint_commits\":{},\"checkpoint_aborts\":{}}}",
            self.jobs_started,
            self.jobs_stopped,
            self.ingested_rows,
            self.emitted_rows,
            self.queue_items,
            self.queue_bytes,
            self.watermark_lag_micros,
            self.checkpoint_duration_micros,
            self.checkpoint_bytes,
            self.checkpoint_commits,
            self.checkpoint_aborts
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_budgeted_and_structured() {
        let m = RuntimeMetrics::new();
        m.record_ingest(3);
        m.record_checkpoint(Duration::from_millis(2), 128);
        let s = m.snapshot();
        assert_eq!(s.ingested_rows, 3);
        assert_eq!(s.checkpoint_bytes, 128);
        assert_eq!(s.checkpoint_commits, 1);
        let line = s.log_line();
        assert!(line.contains("\"event\":\"sparrow_metrics\""));
        assert!(!line.contains("device_id"));
    }
}

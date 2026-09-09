//! Concurrent capture sink with an optional stall gate (G2 fairness).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use sparrow_model::{Row, Scalar, Schema};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub struct StallGate {
    stalled: Arc<AtomicBool>,
    go: Arc<Notify>,
}

impl StallGate {
    pub fn stall(&self) {
        self.stalled.store(true, Ordering::SeqCst);
    }

    pub fn release(&self) {
        self.stalled.store(false, Ordering::SeqCst);
        self.go.notify_waiters();
    }

    pub async fn wait_if_stalled(&self, cancel: &CancellationToken) {
        while self.stalled.load(Ordering::SeqCst) {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = self.go.notified() => {}
            }
        }
    }
}

/// How captured rows are retained. Production live path must not keep
/// every output row forever.
#[derive(Clone, Copy, Debug)]
pub enum CaptureMode {
    /// Counters only — production supervisor default.
    Disabled,
    /// Bounded debug ring (oldest dropped).
    Bounded { max_rows: usize },
    /// Unbounded retain — tests / CLI demos only.
    Unbounded,
}

impl Default for CaptureMode {
    fn default() -> Self {
        Self::Unbounded
    }
}

#[derive(Clone)]
pub struct SharedCapture {
    rows: Arc<Mutex<Vec<Vec<Scalar>>>>,
    late: Arc<Mutex<Vec<Vec<Scalar>>>>,
    schema: Arc<Mutex<Option<Schema>>>,
    seen: Arc<std::sync::atomic::AtomicU64>,
    late_seen: Arc<std::sync::atomic::AtomicU64>,
    mode: CaptureMode,
    pub stall: StallGate,
}

impl Default for SharedCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedCapture {
    /// Unbounded retain (tests / in-process demos).
    pub fn new() -> Self {
        Self::with_mode(CaptureMode::Unbounded)
    }

    /// Production path: counters only, no row retention.
    pub fn disabled() -> Self {
        Self::with_mode(CaptureMode::Disabled)
    }

    pub fn bounded(max_rows: usize) -> Self {
        Self::with_mode(CaptureMode::Bounded {
            max_rows: max_rows.max(1),
        })
    }

    pub fn with_mode(mode: CaptureMode) -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            late: Arc::new(Mutex::new(Vec::new())),
            schema: Arc::new(Mutex::new(None)),
            seen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            late_seen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            mode,
            stall: StallGate::default(),
        }
    }

    pub fn mode(&self) -> CaptureMode {
        self.mode
    }

    pub fn retains_rows(&self) -> bool {
        !matches!(self.mode, CaptureMode::Disabled)
    }

    fn retain(buf: &mut Vec<Vec<Scalar>>, mode: CaptureMode, row: Vec<Scalar>) {
        match mode {
            CaptureMode::Disabled => {}
            CaptureMode::Unbounded => buf.push(row),
            CaptureMode::Bounded { max_rows } => {
                if buf.len() >= max_rows {
                    buf.remove(0);
                }
                buf.push(row);
            }
        }
    }

    pub fn push(&self, schema: &Schema, batch_rows: &[Row]) {
        self.seen
            .fetch_add(batch_rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let mut g = self.rows.lock().expect("capture");
        for r in batch_rows {
            Self::retain(&mut g, self.mode, r.values.clone());
        }
        *self.schema.lock().expect("schema") = Some(schema.clone());
    }

    pub fn push_late(&self, batch_rows: &[Row]) {
        self.late_seen
            .fetch_add(batch_rows.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let mut g = self.late.lock().expect("late");
        for r in batch_rows {
            Self::retain(&mut g, self.mode, r.values.clone());
        }
    }

    pub fn row_count(&self) -> usize {
        match self.mode {
            CaptureMode::Disabled => self.seen.load(std::sync::atomic::Ordering::Relaxed) as usize,
            _ => self.rows.lock().expect("capture").len(),
        }
    }

    pub fn seen_count(&self) -> u64 {
        self.seen.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn late_count(&self) -> usize {
        match self.mode {
            CaptureMode::Disabled => {
                self.late_seen.load(std::sync::atomic::Ordering::Relaxed) as usize
            }
            _ => self.late.lock().expect("late").len(),
        }
    }

    pub fn rows(&self) -> Vec<Vec<Scalar>> {
        self.rows.lock().expect("capture").clone()
    }

    pub fn late_rows(&self) -> Vec<Vec<Scalar>> {
        self.late.lock().expect("late").clone()
    }

    pub fn rows_as_debug(&self) -> Vec<Vec<String>> {
        self.rows()
            .into_iter()
            .map(|r| r.iter().map(fmt_scalar).collect())
            .collect()
    }
}

pub fn fmt_scalar(s: &Scalar) -> String {
    match s {
        Scalar::Null => "null".into(),
        Scalar::Bool(v) => v.to_string(),
        Scalar::Int64(v) => v.to_string(),
        Scalar::UInt64(v) => v.to_string(),
        Scalar::Float64(v) => format!("{v}"),
        Scalar::Utf8(v) => v.to_string(),
        Scalar::Bytes(v) => format!("bytes[{}]", v.len()),
        Scalar::TimestampMicrosUTC(v) => format!("ts:{v}"),
        Scalar::Dynamic(d) => format!("dynamic:{d:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_model::{DataType, Field, FieldId, SchemaId};

    fn schema() -> Schema {
        Schema::new(
            SchemaId::new(1),
            vec![Field::new(FieldId::new(1), "v", DataType::Int64, false)],
        )
        .unwrap()
    }

    #[test]
    fn r03_disabled_does_not_retain_rows() {
        let c = SharedCapture::disabled();
        let schema = schema();
        for i in 0..100 {
            c.push(
                &schema,
                &[Row {
                    values: vec![sparrow_model::Scalar::Int64(i)],
                }],
            );
        }
        assert_eq!(c.seen_count(), 100);
        assert!(c.rows().is_empty());
        assert!(!c.retains_rows());
    }

    #[test]
    fn r03_bounded_ring_drops_oldest() {
        let c = SharedCapture::bounded(3);
        let schema = schema();
        for i in 0..5 {
            c.push(
                &schema,
                &[Row {
                    values: vec![sparrow_model::Scalar::Int64(i)],
                }],
            );
        }
        assert_eq!(c.rows().len(), 3);
        assert_eq!(c.rows()[0][0], sparrow_model::Scalar::Int64(2));
    }
}

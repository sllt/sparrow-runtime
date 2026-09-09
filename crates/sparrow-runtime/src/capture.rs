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

#[derive(Clone, Default)]
pub struct SharedCapture {
    rows: Arc<Mutex<Vec<Vec<Scalar>>>>,
    schema: Arc<Mutex<Option<Schema>>>,
    pub stall: StallGate,
}

impl SharedCapture {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, schema: &Schema, batch_rows: &[Row]) {
        let mut g = self.rows.lock().expect("capture");
        for r in batch_rows {
            g.push(r.values.clone());
        }
        *self.schema.lock().expect("schema") = Some(schema.clone());
    }

    pub fn row_count(&self) -> usize {
        self.rows.lock().expect("capture").len()
    }

    pub fn rows(&self) -> Vec<Vec<Scalar>> {
        self.rows.lock().expect("capture").clone()
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

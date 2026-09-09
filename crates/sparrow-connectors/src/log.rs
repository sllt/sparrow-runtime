use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use sparrow_formats::encode_json_row;
use sparrow_model::{RestoreClaim, RowBatch};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::capabilities::{refuse_durable_recovery, ConnectorCapabilities};
use crate::diag::IoDiagnostics;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct LogSinkConfig {
    pub restore: RestoreClaim,
}

impl Default for LogSinkConfig {
    fn default() -> Self {
        Self {
            restore: RestoreClaim::None,
        }
    }
}

impl LogSinkConfig {
    pub fn capabilities() -> ConnectorCapabilities {
        ConnectorCapabilities::LOG_SINK
    }

    pub fn validate(&self) -> Result<()> {
        refuse_durable_recovery(&self.restore)
    }
}

/// Writes encoded JSON lines to an in-memory ring (bounded) and stderr.
pub struct LogSink {
    pub diag: Arc<IoDiagnostics>,
    lines: Arc<Mutex<Vec<String>>>,
    max_lines: usize,
}

impl LogSink {
    pub fn new(diag: Arc<IoDiagnostics>, max_lines: usize) -> Self {
        Self {
            diag,
            lines: Arc::new(Mutex::new(Vec::new())),
            max_lines: max_lines.max(1),
        }
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("log").clone()
    }

    pub fn shared_lines(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.lines)
    }

    /// Connector SDK flush. Log lines are already in the ring; this is a
    /// barrier no-op so experimental checkpoint can wait on sink flush.
    pub fn flush(&self) -> Result<()> {
        drop(self.lines.lock().expect("log"));
        Ok(())
    }

    pub async fn run(self, mut rx: mpsc::Receiver<RowBatch>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                next = rx.recv() => {
                    match next {
                        Some(batch) => self.write_batch(&batch),
                        None => break,
                    }
                }
            }
        }
    }

    fn write_batch(&self, batch: &RowBatch) {
        let schema = batch.schema();
        let mut guard = self.lines.lock().expect("log");
        for row in batch.rows() {
            if let Ok(bytes) = encode_json_row(schema, row) {
                let line = String::from_utf8_lossy(&bytes).into_owned();
                eprintln!("sparrow-log {line}");
                if guard.len() >= self.max_lines {
                    guard.remove(0);
                }
                guard.push(line);
                self.diag.log_written.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

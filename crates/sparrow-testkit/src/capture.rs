//! Deterministic capture sink stub.

use sparrow_io::RecordSink;
use sparrow_model::{Result, RowBatch, Scalar};

#[derive(Default)]
pub struct CaptureSink {
    pub batches: Vec<RowBatch>,
}

impl CaptureSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn rows_as_debug(&self) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        for batch in &self.batches {
            for row in batch.rows() {
                out.push(row.values.iter().map(fmt_scalar).collect());
            }
        }
        out
    }
}

impl RecordSink for CaptureSink {
    fn send(&mut self, batch: RowBatch) -> Result<()> {
        self.batches.push(batch);
        Ok(())
    }
}

fn fmt_scalar(s: &Scalar) -> String {
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

//! Source frame and codec boundary types.
//!
//! Bytes enter the runtime as a [`SourceFrame`]. A codec (defined in
//! `sparrow-io`) turns frames into typed records. Size limits are enforced
//! here so connectors cannot bypass the bound.

use crate::error::{ErrorCode, Result, SparrowError};

/// Default per-record payload cap (1 MiB). Codecs must not exceed this
/// unless a job opts into a tighter or (still bounded) custom limit.
pub const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;

/// Bytes-in boundary object. Typed records come *out* of a codec, never
/// from this type directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFrame {
    pub payload: Vec<u8>,
    /// Ingest timestamp in microseconds UTC (virtual or wall).
    pub ingested_at_micros: i64,
    pub headers: Vec<(String, String)>,
}

impl SourceFrame {
    pub fn new(payload: Vec<u8>, ingested_at_micros: i64) -> Self {
        Self {
            payload,
            ingested_at_micros,
            headers: Vec::new(),
        }
    }

    pub fn byte_len(&self) -> usize {
        self.payload.len()
    }
}

/// Hard limits applied at the codec boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodecBounds {
    pub max_record_bytes: usize,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
}

impl Default for CodecBounds {
    fn default() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_batch_rows: 256,
            max_batch_bytes: DEFAULT_MAX_RECORD_BYTES,
        }
    }
}

impl CodecBounds {
    pub fn check_frame(&self, frame: &SourceFrame) -> Result<()> {
        if frame.byte_len() > self.max_record_bytes {
            return Err(SparrowError::new(
                ErrorCode::MaxRecordSize,
                format!(
                    "source frame {} bytes exceeds max_record_bytes {}",
                    frame.byte_len(),
                    self.max_record_bytes
                ),
            )
            .context("bytes", frame.byte_len().to_string())
            .context("max_record_bytes", self.max_record_bytes.to_string()));
        }
        Ok(())
    }

    pub fn check_batch_shape(&self, rows: usize, bytes: usize) -> Result<()> {
        if rows > self.max_batch_rows {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("batch rows {rows} exceeds max_batch_rows {}", self.max_batch_rows),
            ));
        }
        if bytes > self.max_batch_bytes {
            return Err(SparrowError::new(
                ErrorCode::BoundExceeded,
                format!("batch bytes {bytes} exceeds max_batch_bytes {}", self.max_batch_bytes),
            ));
        }
        Ok(())
    }
}

/// Marker for "bytes in, typed records out". The actual decoder trait lives
/// in `sparrow-io` so the model crate stays free of I/O traits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodecBoundary;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversize_frame() {
        let bounds = CodecBounds {
            max_record_bytes: 8,
            ..CodecBounds::default()
        };
        let frame = SourceFrame::new(vec![0; 9], 0);
        assert_eq!(bounds.check_frame(&frame).unwrap_err().code, ErrorCode::MaxRecordSize);
    }
}

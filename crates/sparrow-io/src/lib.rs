//! I/O *contracts* for M0. No MQTT, HTTP, or filesystem connectors.
//!
//! Codecs convert [`SourceFrame`] bytes into typed [`RowBatch`] records.
//! Implementations in this crate are test doubles only.

use sparrow_model::{
    CodecBounds, Result, RowBatch, SourceFrame, SparrowError,
};
use sparrow_model::error::ErrorCode;

/// Bytes-in → typed-records-out boundary.
pub trait Decoder {
    fn decode(&mut self, frame: &SourceFrame, bounds: &CodecBounds) -> Result<RowBatch>;
}

/// Pull-style finite or live source. M0 only needs finite fixtures.
pub trait RecordSource {
    fn next_frame(&mut self) -> Result<Option<SourceFrame>>;
}

/// Push-style sink. The capture sink in `sparrow-testkit` is the M0 impl.
pub trait RecordSink {
    fn send(&mut self, batch: RowBatch) -> Result<()>;
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Helper used by decoders before they allocate a batch.
pub fn enforce_frame_bounds(frame: &SourceFrame, bounds: &CodecBounds) -> Result<()> {
    bounds.check_frame(frame)
}

pub fn reject_live_connector(kind: &str) -> SparrowError {
    SparrowError::new(
        ErrorCode::FeatureUnavailable,
        format!("{kind} connector is out of scope for M0"),
    )
    .context("connector", kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_is_not_in_m0() {
        let err = reject_live_connector("mqtt");
        assert_eq!(err.code, ErrorCode::FeatureUnavailable);
    }
}

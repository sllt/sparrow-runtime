//! Input quarantine is a durable disposition, not a source acknowledgement.
//! Only record-local decode failures may use it; resource/runtime failures
//! must still stop the attempt. Checkpoints remain the authority for ACKs.
use crate::SourcePosition;
use sparrow_model::{ErrorCode, Result};
pub fn quarantine_full(error: &sparrow_model::SparrowError) -> bool {
    error.code == ErrorCode::ResourceExhausted
        && error
            .context
            .iter()
            .any(|(key, value)| key == "input_dlq_full" && value == "true")
}

pub fn record_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::InvalidArgument
            | ErrorCode::InvalidSchema
            | ErrorCode::TypeMismatch
            | ErrorCode::CodecViolation
            | ErrorCode::BoundExceeded
            | ErrorCode::MaxRecordSize
    )
}

pub trait InputQuarantine: Send + Sync {
    /// Bind the source incarnation and refuse replay below an explicitly
    /// retired history floor. Called before admitting any input.
    fn check_start(&self, position: &SourcePosition) -> Result<()>;
    /// Blocking, bounded FULL commit. The position is AFTER this record.
    fn capture(&self, position: &SourcePosition, payload: &[u8], code: ErrorCode) -> Result<()>;
}

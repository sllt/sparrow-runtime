//! Structured errors with a stable code, retry hint, and key/value context.
//!
//! Job-level failure attribution is in-process (not process isolation). Callers
//! should attach `pipeline` / `job_attempt` / `operator` context when known.

use std::fmt;

use crate::ids::{JobAttemptId, OperatorId, PipelineId};

/// Stable machine-readable error class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    InvalidArgument,
    InvalidSchema,
    TypeMismatch,
    BoundExceeded,
    ResourceExhausted,
    FeatureUnavailable,
    UnsupportedRestore,
    UnsupportedDelivery,
    CodecViolation,
    MaxRecordSize,
    JobFailed,
    Cancelled,
    PolicyDenied,
    SecretMissing,
    Internal,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::InvalidSchema => "invalid_schema",
            Self::TypeMismatch => "type_mismatch",
            Self::BoundExceeded => "bound_exceeded",
            Self::ResourceExhausted => "resource_exhausted",
            Self::FeatureUnavailable => "feature_unavailable",
            Self::UnsupportedRestore => "unsupported_restore",
            Self::UnsupportedDelivery => "unsupported_delivery",
            Self::CodecViolation => "codec_violation",
            Self::MaxRecordSize => "max_record_size",
            Self::JobFailed => "job_failed",
            Self::Cancelled => "cancelled",
            Self::PolicyDenied => "policy_denied",
            Self::SecretMissing => "secret_missing",
            Self::Internal => "internal",
        }
    }

    /// Conservative default: only resource pressure is retryable.
    pub const fn default_retryable(self) -> bool {
        matches!(self, Self::ResourceExhausted)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured Sparrow error. Prefer this over ad-hoc strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparrowError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub context: Vec<(String, String)>,
}

impl SparrowError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            retryable: code.default_retryable(),
            code,
            message: message.into(),
            context: Vec::new(),
        }
    }

    pub fn retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.push((key.into(), value.into()));
        self
    }

    pub fn at_job(self, pipeline: PipelineId, attempt: JobAttemptId) -> Self {
        self.context("pipeline", pipeline.to_string())
            .context("job_attempt", attempt.to_string())
    }

    pub fn at_operator(self, operator: OperatorId) -> Self {
        self.context("operator", operator.to_string())
    }
}

impl fmt::Display for SparrowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)?;
        if !self.context.is_empty() {
            write!(f, " [")?;
            for (i, (k, v)) in self.context.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{k}={v}")?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

impl std::error::Error for SparrowError {}

pub type Result<T> = std::result::Result<T, SparrowError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_error_carries_code_and_context() {
        let err = SparrowError::new(ErrorCode::BoundExceeded, "row cap")
            .retryable(false)
            .context("max_rows", "64")
            .at_operator(OperatorId::new(3));
        assert_eq!(err.code, ErrorCode::BoundExceeded);
        assert!(!err.retryable);
        assert!(err.to_string().contains("operator=OperatorId(3)"));
    }
}

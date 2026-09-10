//! Typed desired/actual pipeline status at the Rust boundary.
//!
//! SQLite still stores `desired_status` / `actual_status` as TEXT (no
//! catalog migration). [`PipelineStatus`] is the only vocabulary the
//! control plane accepts on write and returns on read.

use serde::{Deserialize, Serialize};
use sparrow_model::{ErrorCode, Result, SparrowError};

/// Closed status vocabulary for desired and actual pipeline rows.
///
/// Desired writes are `running` | `stopped`. Actual also uses
/// `starting`, `waiting` (capacity), `failed`, and `completed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStatus {
    Stopped,
    Starting,
    Running,
    Waiting,
    Failed,
    Completed,
}

impl PipelineStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Failed => "failed",
            Self::Completed => "completed",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "stopped" => Ok(Self::Stopped),
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "waiting" => Ok(Self::Waiting),
            "failed" => Ok(Self::Failed),
            "completed" => Ok(Self::Completed),
            other => Err(SparrowError::new(
                ErrorCode::InvalidArgument,
                format!("unknown pipeline status '{other}'"),
            )),
        }
    }

    /// Statuses the catalog may write as *desired* state.
    pub const fn is_desired(self) -> bool {
        matches!(self, Self::Running | Self::Stopped)
    }
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<str> for PipelineStatus {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for PipelineStatus {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl AsRef<str> for PipelineStatus {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a8_pipeline_status_rejects_unknown() {
        assert_eq!(PipelineStatus::parse("running").unwrap(), PipelineStatus::Running);
        assert_eq!(
            PipelineStatus::parse("bogus").unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(PipelineStatus::Failed.as_str(), "failed");
        assert!(PipelineStatus::Running.is_desired());
        assert!(!PipelineStatus::Starting.is_desired());
    }
}

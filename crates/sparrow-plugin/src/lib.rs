//! Explicitly trusted, immutable native scalar packages. No process sandbox.
mod manifest;
mod native;
mod registry;
pub use manifest::*;
pub use native::{resident_count, MAX_RESIDENT};
pub use registry::{Function, Manager, PackageInfo};
use sparrow_model::{ErrorCode, SparrowError};
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}

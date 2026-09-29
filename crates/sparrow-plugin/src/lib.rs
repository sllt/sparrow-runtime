//! Immutable scalar packages: trusted native ABI or process-bounded JavaScript.
mod manifest;
mod native;
mod registry;
pub mod script;
#[doc(hidden)]
pub mod worker;
pub use manifest::*;
pub use native::{resident_count, MAX_RESIDENT};
pub use registry::{Function, Manager, PackageInfo};
use sparrow_model::{ErrorCode, SparrowError};
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}

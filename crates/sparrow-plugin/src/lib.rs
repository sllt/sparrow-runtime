//! Immutable scalar packages: trusted native ABI or process-bounded JavaScript.
mod manifest;
mod native;
mod native_dependencies;
mod registry;
pub mod script;
pub mod trust;
pub mod extension;
#[doc(hidden)]
pub mod worker;
pub use manifest::*;
pub use native::{resident_count, MAX_RESIDENT};
pub use registry::{Function, Manager, PackageInfo, Extension};
use sparrow_model::{ErrorCode, SparrowError};
fn invalid(s: &str) -> SparrowError {
    SparrowError::new(ErrorCode::InvalidArgument, s)
}

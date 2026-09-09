//! Sparrow shared model: IDs, types, errors, delivery/resource vocabulary,
//! and the *provisional* RowBatch / MemoryLease ownership prototype.
//!
//! This crate has **no Tokio dependency** and no server stack.
//!
//! [`batch::RowBatch`] is the V0.1 default layout (ADR-003). Packing may
//! still change internally; it is not a public ABI. Arrow stays in
//! `experiments/` only.

pub mod batch;
pub mod budget;
pub mod delivery;
pub mod error;
pub mod frame;
pub mod ids;
pub mod memory;
pub mod resource;
pub mod scalar;
pub mod types;

pub use batch::{Row, RowBatch, RowBatchBuilder};
pub use budget::WorkBudget;
pub use delivery::{DeliveryContract, DeliveryGuarantee, RecoveryPolicy, RestoreClaim};
pub use error::{ErrorCode, Result, SparrowError};
pub use frame::{CodecBounds, CodecBoundary, SourceFrame, DEFAULT_MAX_RECORD_BYTES};
pub use ids::{FieldId, JobAttemptId, OperatorId, PipelineId, RevisionId, SchemaId};
pub use memory::{MemoryLease, MemoryOwner};
pub use resource::{CreditKind, CreditUsage, ResourceBudget};
pub use scalar::{DynamicValue, Scalar};
pub use types::{DataType, Field, Schema};

#[cfg(test)]
mod ownership_proptests;

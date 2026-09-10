//! Thin control plane: SQLite catalog + desired→actual supervisor.
//!
//! This crate depends on runtime/connectors/sql. `sparrow-runtime` must not
//! depend on this crate, SQLite, or Axum.

pub mod spec;
pub mod store;
pub mod supervisor;
pub mod validate;

pub use spec::{PipelineSpec, RestoreSpec, SinkSpec, SourceSpec, StreamSpec};
pub use store::{
    ActualState, AuditRow, DesiredState, PipelineRow, Store, CATALOG_SCHEMA_VERSION, FORMAT_VERSION,
};
pub use supervisor::{
    compact_kernel, host_kernel, request_start, request_start_at, request_stop, DemoHarness,
    Supervisor,
};
pub use validate::{
    bind_plan, binder_catalog, capabilities_json, effective_guarantees, explain_plan, honesty_json,
    store_policy, stream_schema, stream_to_schema, validate_aligned_plan, validate_io,
    DemoEndpoints, ExplainReport, StoreSecrets, HONESTY,
};

#[cfg(test)]
mod review_tests;

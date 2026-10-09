//! Thin control plane: SQLite catalog + desired→actual supervisor.
//!
//! This crate depends on runtime/connectors/sql. `sparrow-runtime` must not
//! depend on this crate, SQLite, or Axum.

#[cfg(test)]
mod analysis_tests;
#[cfg(test)]
mod plugin_tests;
#[cfg(test)]
mod extension_tests;
pub mod capability;
pub mod checkpoint;
pub mod query;
pub mod lookup;
pub mod plugins;
pub mod reference_table;
pub mod spec;
pub mod status;
pub mod store;
pub mod supervisor;
pub mod validate;

mod file_source;
mod plugin_io;

pub use reference_table::{
    reference_table_sha256, MutationSpec, ReferenceTableMetadata, ReferenceTableRow,
    ReferenceTableSpec, RollbackSpec, TableMutation, MAX_REFERENCE_TABLE_MUTATIONS,
    MAX_REFERENCE_TABLE_BYTES, MAX_REFERENCE_TABLE_BYTES_PER_NAME,
    MAX_REFERENCE_TABLE_CATALOG_BYTES, MAX_REFERENCE_TABLE_NAMES, MAX_REFERENCE_TABLE_PREVIEW_PINS,
    MAX_REFERENCE_TABLE_ROWS, MAX_REFERENCE_TABLE_VERSIONS, REFERENCE_TABLE_METADATA_BYTES,
};
pub use spec::{
    HttpPollAuthSpec, HttpPollHeaderSpec, HttpPollSpec, PipelineSpec, ReferenceBinding,
    RestoreSpec, SinkSpec, SourceSpec, StreamSpec,
};
#[cfg(all(test, feature = "demo-io", target_os = "linux"))]
mod actions_tests;
#[cfg(all(test, feature = "demo-io", target_os = "linux"))]
mod csv_tests;
#[cfg(all(test, feature = "demo-io"))]
mod k3_tests;
#[cfg(all(test, feature = "demo-io"))]
mod k4_tests;
#[cfg(all(test, feature = "demo-io"))]
mod paused_time_tests;
#[cfg(all(test, feature = "demo-io"))]
mod time_graph_tests;
#[cfg(all(test, feature = "demo-io"))]
mod window_completion_tests;
pub use checkpoint::CheckpointSpec;
pub use status::PipelineStatus;
pub use store::{
    secrets_key_configured, secrets_key_required, ActualState, AuditRow, DesiredState, PipelineRow,
    Store, CATALOG_SCHEMA_VERSION, FORMAT_VERSION,
};
#[cfg(feature = "demo-io")]
pub use supervisor::DemoHarness;
pub use supervisor::{
    compact_kernel, host_kernel, host_kernel_with_max_jobs, parse_max_jobs, request_start,
    request_start_at, request_stop, DemoIo, Supervisor,
};
pub use validate::{
    bind_plan, bind_plan_with_store, binder_catalog, capabilities_json, effective_guarantees,
    effective_guarantees_with_plan, explain_plan, explain_plan_with, honesty_json,
    replay_label_for_source, resolve_file_contract, store_policy, stream_schema, stream_to_schema,
    validate_aligned_plan, validate_io, DemoEndpoints, ExplainReport, StoreSecrets, HONESTY,
};

#[cfg(test)]
mod core_b_binding_tests;
#[cfg(test)]
mod databus_tests;
#[cfg(test)]
mod reference_tests;
#[cfg(test)]
mod live_lookup_tests;
#[cfg(test)]
mod reference_mutation_tests;

#[cfg(test)]
mod review_tests;

#[cfg(test)]
mod r4_tests;

#[cfg(all(test, feature = "jetstream", feature = "demo-io"))]
mod core_a_tests;
#[cfg(all(test, feature = "demo-io"))]
mod core_b2_tests;
#[cfg(all(test, feature = "demo-io"))]
mod http_poll_tests;
#[cfg(all(test, feature = "demo-io"))]
mod hysteresis_completion_tests;
#[cfg(test)]
mod jetstream_sink_tests;
#[cfg(all(test, feature = "jetstream", feature = "demo-io"))]
mod k2_tests;
#[cfg(test)]
mod nats_tests;
#[cfg(test)]
mod reference_completion_tests;

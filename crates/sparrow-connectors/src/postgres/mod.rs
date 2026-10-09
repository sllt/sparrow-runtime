//! PostgreSQL: a periodic incremental query Source, an INSERT/UPSERT Sink
//! and a Lookup provider over tokio-postgres (pinned 0.7.18, no libpq) with
//! the crate's rustls for TLS. No logical replication / CDC.
//!
//! The client crate does the protocol (extended query protocol only, so a
//! configured query is exactly one statement); this module adds what it does
//! not: a bound on every backend message ([`conn::Guarded`]), explicit
//! cancel requests for abandoned statements ([`conn::CancelOnDrop`]; a
//! dropped tokio-postgres future does not stop the server), an explicit type
//! mapping ([`types`]) and job memory accounting.

pub mod conn;
pub mod lookup;
pub mod sink;
pub mod source;
pub mod types;

pub use conn::{PgSslMode, PgTarget};
pub use lookup::{PgLookup, PgLookupConfig};
pub use sink::{PgSink, PgSinkConfig, PgWriteMode};
pub use source::{PgSource, PgSourceConfig};

#[cfg(test)]
use crate::http::observation_tls_fixture as tls_fixture;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod real_tests;

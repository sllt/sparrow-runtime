//! Apache Kafka consumer-group Source and producer Sink (`kafka` feature).
//!
//! Client: `rdkafka =0.39.0` over librdkafka 2.12.1, built from source
//! (plaintext only, gzip via a static zlib; no SSL/SASL in this build).
//! See `docs/KAFKA.md` for the contract; in short:
//!
//! - Source: consumer group, offsets committed to Kafka only for messages
//!   whose row was admitted into the job inbox (or skipped as poison under
//!   the skip policy). Commits carry an identity (cluster id, topic, group,
//!   partition count, payload format) and a mismatching or foreign commit is
//!   refused instead of resumed.
//! - Sink: one record per row, delivery reports awaited before the batch is
//!   acknowledged; idempotent `acks=all` by default; fail closed on an
//!   unconfirmed delivery.

mod client;
pub mod sink;
pub mod source;

#[cfg(test)]
pub(crate) mod broker;

/// The pinned client crate, for the control-plane broker tests (which
/// include `broker.rs` by path).
#[doc(hidden)]
pub use rdkafka;
#[cfg(test)]
mod tests;

pub use client::{KafkaClientConfig, MAX_BROKERS};
pub use sink::{KafkaAcks, KafkaSink, KafkaSinkConfig};
pub use source::{KafkaSource, KafkaSourceConfig, OffsetReset};

use sparrow_model::{ErrorCode, SparrowError};

pub(crate) fn error(code: ErrorCode, message: impl Into<String>) -> SparrowError {
    SparrowError::new(code, message)
}

/// Topic / group names: Kafka's legal characters and length.
pub(crate) fn check_name(what: &str, name: &str, max: usize) -> sparrow_model::Result<()> {
    if name.is_empty()
        || name.len() > max
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(error(
            ErrorCode::InvalidArgument,
            format!("Kafka {what} must be 1..={max} of [A-Za-z0-9._-] (not . or ..)"),
        ));
    }
    Ok(())
}

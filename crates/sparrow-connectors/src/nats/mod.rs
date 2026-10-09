//! NATS Core (non-JetStream) live Source and Sink, plus the client setup
//! shared with the JetStream adapter. Contract: `docs/NATS.md`.
//!
//! NATS Core is at-most-once: no acknowledgement, no persistence, no replay.
//! Nothing here is a checkpoint or reliable-delivery claim.

mod client;
pub mod common;
mod sink;
mod source;
mod wire;

pub use client::{BoundToken, NatsClientConfig};
pub use sink::{NatsSink, NatsSinkConfig};
pub use source::{NatsSource, NatsSourceConfig};

#[cfg(test)]
mod tests;

//! TCP client Source and Sink with `lines` or `length_prefixed` framing and
//! optional TLS. Contract: `docs/TCP.md`.
//!
//! Live and at-most-once: no acknowledgement, no persistence, no replay.
//! Client mode only; the runtime does not listen for TCP connections.

mod client;
mod framing;
mod sink;
mod source;

pub use crate::net::reconnect_delay;
pub use client::{BoundTcp, TcpClientConfig, MAX_FRAME_BYTES, MIN_FRAME_BYTES};
pub use framing::{OversizePolicy, PrefixWidth, TcpFraming};
pub use sink::{TcpOverflow, TcpSink, TcpSinkConfig};
pub use source::{TcpSource, TcpSourceConfig};

#[cfg(test)]
#[path = "../mqtt/tls_fixture.rs"]
mod tls_fixture;

#[cfg(test)]
mod tests;

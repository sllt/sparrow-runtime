//! WebSocket client Source and Sink (`ws://` / `wss://`). Contract:
//! `docs/WEBSOCKET.md`.
//!
//! Live and at-most-once: no acknowledgement, no persistence, no replay.
//! Nothing here is a checkpoint or reliable-delivery claim. Client mode
//! only; the runtime does not listen for WebSocket connections.

pub(crate) mod client;
mod sink;
mod source;

pub use client::{
    reconnect_delay, BoundClient, WebSocketAuth, WebSocketClientConfig, WebSocketHeader,
};
pub use sink::{Overflow, SinkFrame, WebSocketSink, WebSocketSinkConfig};
pub use source::{
    BinaryFrames, WebSocketFraming, WebSocketSource, WebSocketSourceConfig,
    DEFAULT_PREFETCH_CAPACITY,
};

#[cfg(test)]
#[path = "../mqtt/tls_fixture.rs"]
mod tls_fixture;

#[cfg(test)]
mod tests;

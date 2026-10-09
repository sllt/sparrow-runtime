//! Redis: a pipelined Sink and a Lookup provider over a small bounded RESP2
//! client (TCP or TLS, `AUTH [user] password`, `SELECT db`).
//!
//! The client is our own (`resp`, `conn`) rather than a client crate so that
//! every reply is bounded before it is buffered, connections are never
//! re-established or commands re-sent behind the caller's back, and memory is
//! charged to the job before encoding. Redis Cluster and Sentinel are not
//! supported (cluster redirections are refused).

pub mod conn;
pub mod lookup;
pub mod resp;
pub mod sink;
pub mod template;

pub use conn::{Endpoint, RedisTarget};
pub use lookup::{RedisLookup, RedisLookupConfig, RedisLookupFormat};
pub use sink::{HashFields, RedisCommand, RedisSink, RedisSinkConfig, RedisValue};
pub use template::Template;

#[cfg(test)]
#[path = "../mqtt/tls_fixture.rs"]
mod tls_fixture;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod real_tests;

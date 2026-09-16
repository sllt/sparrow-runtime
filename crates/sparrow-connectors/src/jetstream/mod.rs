//! K2 JetStream adapter. SDK ownership and protocol validation stay outside
//! the runtime. This module alone is NOT a checkpoint/reliable-delivery claim.
mod connection;
mod ack;
mod ledger;
mod policy;
mod reader;

pub use connection::{Connection, ConnectionConfig};
use ledger::{DeliveryIdentity, DeliveryLedger, Observation};
pub use policy::{check_stream, StreamIdentity};
pub use reader::{InputRecord, Reader, ReaderConfig, ReaderPoll};

#[cfg(test)]
mod conformance;

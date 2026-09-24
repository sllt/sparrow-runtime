//! Point-in-time feed observations, distinct from cached connection health.
//!
//! These facts describe the connector's observed prefix, not device health or
//! a continuously available interval. Callers must order observations with
//! input, bound their freshness, and persist them in a dedicated decision
//! protocol before using them for recoverable silence detection. They do not
//! extend the meaning of existing source-position or checkpoint codecs.

use crate::SourcePosition;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedReadiness {
    /// A fresh probe found no bytes/messages beyond the reader's prefix, no
    /// unconsumed local delivery, and an eligible ongoing feed.
    CaughtUp,
    /// A positive backlog was observed. It is not evidence of device silence.
    Backlog,
    /// A file record has not reached a complete message boundary.
    PartialRecord,
    /// A fetch or locally received but unpublished delivery is unresolved.
    InFlight,
    /// The platform/source contract cannot establish the required facts.
    Unverified,
    /// A finite source ended; it cannot establish ongoing feed availability.
    Ended,
}

/// A connector observation at a specific source prefix. `head` uses the
/// connector's offset units (File bytes, JetStream stream sequence), not a
/// wall-clock timestamp. Errors remain errors, never a successful CaughtUp.
///
/// The caller accounts the owned position/identity strings in its bounded
/// control workspace, as it does for an ordinary source cut. This object is
/// not itself a durable record and carries no replay/availability guarantee.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedObservation {
    pub position: SourcePosition,
    pub head: u64,
    pub readiness: FeedReadiness,
}

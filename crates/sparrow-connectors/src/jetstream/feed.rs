//! Point-in-time JetStream feed observations.
//!
//! A probe is a control-plane read, not a health sample: it re-runs the existing
//! ownership/policy/range verification on fresh server reads, compares the
//! consumer against the identity and configuration captured when this reader
//! created it, and classifies the observed prefix. The caller records the
//! snapshot's own start/end times outside the connector; no local wall clock is
//! read here, and nothing in this module is device-health evidence or an
//! availability claim.
//!
//! `Consumer::get_info` is used deliberately: it returns a fresh server value
//! *without* writing the SDK cache. `cached_info` therefore stays the immutable
//! creation-time baseline for this attempt (identity and configuration), and is
//! never treated as fresh or as a health signal.

use super::{check_stream, error, StreamIdentity};
use sparrow_io::feed::{FeedObservation, FeedReadiness};
use sparrow_model::{ErrorCode, Result};

/// Facts of one probe: fresh server reads plus the local delivery ledger.
struct FeedInputs {
    /// A fetch is still open, so deliveries may be outstanding.
    active_pull: bool,
    /// Local admission/publication/delivery positions.
    received: u64,
    published: u64,
    delivered: u64,
    /// Fresh consumer facts.
    consumer_pending: u64,
    consumer_delivered: u64,
    consumer_stream_position: u64,
    /// Fresh server-side waiting pull count: a fetch the server still holds
    /// open has outstanding deliveries and must not be read as caught up.
    consumer_waiting: u64,
    /// Fresh stream head (`last_sequence`), never a cached value.
    head: u64,
}

/// Classify a completed probe.
///
/// Sequence regressions and impossible positions are errors: they invalidate
/// every derived fact and must never be reported as a successful readiness.
/// Unconfirmed ACKs are deliberately absent from this function: a delivery
/// waiting for its ACK reply is a retry fact, not a feed-readiness fact, and
/// requiring `pending() == 0` would make an ongoing feed permanently
/// unclassifiable. Draining is proven by the head, the published prefix and
/// the consumer's pending/delivered counters, never by `num_pending == 0` alone.
/// A waiting pull on the server (`num_waiting > 0`) is in-flight evidence: the
/// server still holds a fetch for this consumer, so it cannot be caught up.
fn classify(input: &FeedInputs) -> Result<FeedReadiness> {
    if input.received < input.published {
        return Err(error(
            ErrorCode::Internal,
            "JetStream ledger published beyond the admitted prefix",
        ));
    }
    if input.head < input.published {
        return Err(error(
            ErrorCode::UnsupportedRestore,
            "JetStream stream head regressed behind the published prefix",
        ));
    }
    if input.head < input.received {
        return Err(error(
            ErrorCode::UnsupportedRestore,
            "JetStream stream head regressed behind the admitted prefix",
        ));
    }
    if input.consumer_delivered < input.delivered {
        return Err(error(
            ErrorCode::UnsupportedRestore,
            "JetStream consumer delivery sequence regressed",
        ));
    }
    if input.consumer_stream_position > input.head {
        return Err(error(
            ErrorCode::CodecViolation,
            "JetStream consumer delivered beyond the fresh stream head",
        ));
    }
    if !input.active_pull
        && input.consumer_waiting == 0
        && input.received == input.published
        && input.consumer_pending == 0
        && input.head == input.published
        && input.consumer_delivered == input.delivered
        // Restates the contract: the delivered stream position may not sit
        // ahead of the published prefix (an empty consumer reports 0).
        && input.consumer_stream_position <= input.published
    {
        return Ok(FeedReadiness::CaughtUp);
    }
    // An open local fetch, a pull the server is still holding open for this
    // consumer, or an admitted row that is not published is in flight.
    if input.active_pull || input.consumer_waiting > 0 || input.received > input.published {
        return Ok(FeedReadiness::InFlight);
    }
    if input.consumer_pending > 0 || input.head > input.published {
        return Ok(FeedReadiness::Backlog);
    }
    // Nothing is known outstanding, yet the server counted deliveries this
    // reader never observed (or its counters belong to another incarnation).
    // The published prefix cannot be proven delivered, so no readiness is
    // claimed. `PartialRecord` (file boundaries) and `Ended` (finite sources)
    // are not reachable for this live, message-oriented source.
    Ok(FeedReadiness::Unverified)
}

impl super::Reader {
    /// Fresh observation of the input feed at the current published cut.
    ///
    /// `records` is the record count of the same cut the caller is observing.
    /// The probe never fetches, ACKs, publishes, commits or advances a cut, and
    /// a timeout, ownership or identity failure stays an error.
    pub async fn observe_feed(&mut self, records: u64) -> Result<FeedObservation> {
        self.connection.check_health()?;
        // Ownership binding, ownership policy, stream policy and source range
        // are re-verified against fresh server reads (this also re-checks the
        // connection), so a probe cannot rely on a previous Ready verdict.
        self.verify().await?;
        let fresh = self.consumer.get_info().await.map_err(|_| {
            error(
                ErrorCode::JobFailed,
                "JetStream consumer identity read failed",
            )
            .retryable(true)
        })?;
        {
            // Creation-time identity and configuration. The cache is only
            // written by `open` (and never by this probe), so it cannot drift.
            let baseline = self.consumer.cached_info();
            if fresh.name != baseline.name
                || fresh.stream_name != baseline.stream_name
                || fresh.created != baseline.created
                || fresh.config != baseline.config
            {
                return Err(error(
                    ErrorCode::PolicyDenied,
                    "JetStream consumer identity or configuration changed during the attempt",
                ));
            }
        }
        if fresh.name != self.reader_name || fresh.stream_name != self.identity.stream {
            return Err(error(
                ErrorCode::CodecViolation,
                "JetStream consumer no longer matches the verified reader/stream",
            ));
        }
        let head = {
            let info = self.stream.info().await.map_err(|_| {
                error(ErrorCode::JobFailed, "JetStream stream head read failed").retryable(true)
            })?;
            if StreamIdentity::from_info(&self.config.namespace, info)?
                .with_reader(&self.config.ownership_bucket, &self.config.consumer)
                != self.identity
            {
                return Err(error(
                    ErrorCode::UnsupportedRestore,
                    "JetStream stream recreated during attempt",
                ));
            }
            check_stream(
                info,
                self.ledger.committed().checked_add(1).ok_or_else(|| {
                    error(
                        ErrorCode::BoundExceeded,
                        "JetStream source sequence exhausted",
                    )
                })?,
            )?;
            info.state.last_sequence
        };
        let readiness = classify(&FeedInputs {
            active_pull: self.pull.is_some(),
            received: self.ledger.received(),
            published: self.ledger.published(),
            delivered: self.ledger.delivered(),
            consumer_pending: fresh.num_pending,
            consumer_delivered: fresh.delivered.consumer_sequence,
            consumer_stream_position: fresh.delivered.stream_sequence,
            consumer_waiting: fresh.num_waiting as u64,
            head,
        })?;
        let position = self.position(records);
        self.connection.check_health()?;
        Ok(FeedObservation {
            position,
            head,
            readiness,
        })
    }
}

#[cfg(test)]
#[path = "feed_tests.rs"]
mod feed_tests;

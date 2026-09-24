//! Boundary tests for the JetStream feed classifier (no broker).
//!
//! Included from `feed.rs` as `mod feed_tests`. These exercise the pure
//! readiness decision and, through the real delivery ledger, the rule that an
//! unconfirmed ACK never blocks a `CaughtUp` verdict.

use super::super::AckToken;
use super::*;
use crate::jetstream::ledger::{DeliveryIdentity, DeliveryLedger, Observation};
use sparrow_model::{MemoryOwner, ResourceBudget};

/// Fully drained prefix: no fetch open, nothing unpublished, head reached and
/// the server's delivered counters agree with the ledger.
fn caught_up() -> FeedInputs {
    FeedInputs {
        active_pull: false,
        received: 4,
        published: 4,
        delivered: 4,
        consumer_pending: 0,
        consumer_delivered: 4,
        consumer_stream_position: 4,
        consumer_waiting: 0,
        head: 4,
    }
}

#[test]
fn feed_caught_up_requires_every_drain_condition() {
    assert_eq!(classify(&caught_up()).unwrap(), FeedReadiness::CaughtUp);

    // An open fetch, or an admitted row that is not published, is in flight.
    assert_eq!(
        classify(&FeedInputs {
            active_pull: true,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::InFlight
    );
    assert_eq!(
        classify(&FeedInputs {
            received: 5,
            delivered: 5,
            consumer_delivered: 5,
            consumer_stream_position: 5,
            head: 5,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::InFlight
    );
    // A pull the server still holds open is in-flight evidence even when the
    // local state has nothing outstanding.
    assert_eq!(
        classify(&FeedInputs {
            consumer_waiting: 1,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::InFlight
    );
    // In-flight evidence outranks a known backlog.
    assert_eq!(
        classify(&FeedInputs {
            active_pull: true,
            head: 9,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::InFlight
    );

    // `num_pending == 0` cannot stand in for the stream head: a fresh head
    // beyond the published prefix is a real, unconsumed backlog.
    assert_eq!(
        classify(&FeedInputs {
            head: 5,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::Backlog
    );
    // Undelivered messages are a backlog even when the head is reached.
    assert_eq!(
        classify(&FeedInputs {
            consumer_pending: 3,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::Backlog
    );

    // The server counted a delivery this reader never observed: nothing proves
    // the published prefix was delivered, so no readiness is claimed.
    assert_eq!(
        classify(&FeedInputs {
            consumer_delivered: 5,
            ..caught_up()
        })
        .unwrap(),
        FeedReadiness::Unverified
    );
}

#[test]
fn feed_unconfirmed_ack_is_not_a_readiness_failure() {
    let owner = MemoryOwner::new(ResourceBudget::compact());
    let mut ledger = DeliveryLedger::new(owner.clone(), 8, 64 * 1024, 0).unwrap();
    let (observation, retired) = ledger
        .observe(
            DeliveryIdentity {
                stream_sequence: 1,
                consumer_sequence: 1,
                payload_digest: [9; 32],
                wire_bytes: 8,
            },
            AckToken("fixture.ack".into()),
        )
        .unwrap();
    assert!(matches!(observation, Observation::New) && retired.is_none());
    ledger.publish(1).unwrap();
    assert_eq!(
        ledger.pending(),
        1,
        "the delivery is retained until its ACK"
    );
    assert_eq!(
        (ledger.received(), ledger.published(), ledger.delivered()),
        (1, 1, 1)
    );

    // Received, published and delivered agree and the head is reached, so the
    // feed is caught up even though its ACK has not been confirmed yet.
    assert_eq!(
        classify(&FeedInputs {
            active_pull: false,
            received: ledger.received(),
            published: ledger.published(),
            delivered: ledger.delivered(),
            consumer_pending: 0,
            consumer_delivered: 1,
            consumer_stream_position: 1,
            consumer_waiting: 0,
            head: 1,
        })
        .unwrap(),
        FeedReadiness::CaughtUp
    );
    drop(ledger);
    assert_eq!(owner.usage().physical_bytes, 0);
}

#[test]
fn feed_regressions_and_impossible_positions_are_errors() {
    // The ledger can never publish a row it did not admit.
    let publish_beyond_admission = FeedInputs {
        received: 3,
        ..caught_up()
    };
    assert_eq!(
        classify(&publish_beyond_admission).unwrap_err().code,
        ErrorCode::Internal
    );
    // The stream head fell behind the published prefix: the stream was
    // recreated or lost a committed prefix.
    let head_regression = FeedInputs {
        head: 3,
        ..caught_up()
    };
    assert_eq!(
        classify(&head_regression).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    // An admitted sequence proves the stream once held it, so a head below the
    // admitted prefix is a lost prefix even while the published cut agrees.
    let admitted_beyond_head = FeedInputs {
        received: 5,
        head: 4,
        ..caught_up()
    };
    assert_eq!(
        classify(&admitted_beyond_head).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    // A consumer whose delivery counter moved backwards belongs to another
    // incarnation (deleted/recreated or reset), never to this reader.
    let delivery_regression = FeedInputs {
        consumer_delivered: 3,
        ..caught_up()
    };
    assert_eq!(
        classify(&delivery_regression).unwrap_err().code,
        ErrorCode::UnsupportedRestore
    );
    // A delivery beyond the fresh stream head is impossible for a live stream.
    let beyond_head = FeedInputs {
        consumer_stream_position: 5,
        ..caught_up()
    };
    assert_eq!(
        classify(&beyond_head).unwrap_err().code,
        ErrorCode::CodecViolation
    );
    // Errors are never reported as a successful readiness.
    for input in [head_regression, delivery_regression, beyond_head] {
        assert!(classify(&input).is_err());
    }
}

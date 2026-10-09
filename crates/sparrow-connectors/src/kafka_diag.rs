//! Kafka Source / Sink counters, grouped so the flat `IoSnapshot` stays
//! readable. Present in every build (zero without the `kafka` feature).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use sparrow_model::QueueOccupancy;

macro_rules! kafka_counters {
    ($($(#[$doc:meta])* $name:ident),* $(,)?) => {
        #[derive(Debug, Default)]
        pub struct KafkaCounters {
            $($(#[$doc])* pub $name: AtomicU64,)*
            /// Source inbox occupancy (decoded rows not yet taken by the job).
            pub source_inbox: Arc<QueueOccupancy>,
        }

        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub struct KafkaSnapshot {
            $($(#[$doc])* pub $name: u64,)*
            pub source_inbox_items: u64,
            pub source_inbox_bytes: u64,
        }

        impl KafkaCounters {
            pub fn snapshot(&self) -> KafkaSnapshot {
                KafkaSnapshot {
                    $($name: self.$name.load(Ordering::Relaxed),)*
                    source_inbox_items: self.source_inbox.items.load(Ordering::Relaxed),
                    source_inbox_bytes: self.source_inbox.bytes.load(Ordering::Relaxed),
                }
            }
        }

        impl KafkaSnapshot {
            pub fn add_assign(&mut self, other: &KafkaSnapshot) {
                $(self.$name += other.$name;)*
                self.source_inbox_items += other.source_inbox_items;
                self.source_inbox_bytes += other.source_inbox_bytes;
            }

            /// `(name, value)` pairs for metrics export.
            pub fn pairs(&self) -> Vec<(&'static str, u64)> {
                vec![
                    $((stringify!($name), self.$name),)*
                    ("source_inbox_items", self.source_inbox_items),
                    ("source_inbox_bytes", self.source_inbox_bytes),
                ]
            }
        }
    };
}

kafka_counters! {
    /// Messages handed out by the consumer (before any check).
    source_received,
    /// Rows admitted into the job inbox (each one is then committable).
    source_rows,
    /// Poison messages (undecodable or oversize) skipped under the skip
    /// policy; each is committed past.
    source_poison_skipped,
    /// Messages over `max_message_bytes` / inbox row bound (poison).
    source_oversize,
    /// Waits for reservation credit (decode / payload copy); never a drop.
    source_budget_waits,
    /// Waits for a full inbox.
    source_backpressure_waits,
    /// Offset commit requests issued (periodic async, revoke and stop sync)
    /// and failed (sync result or async callback; retried while uncommitted).
    source_commits,
    source_commit_failed,
    /// Partitions assigned / revoked by group rebalances.
    source_assigned,
    source_revoked,
    /// Revocations where the assignment was lost (no commit attempted).
    source_lost,
    /// Consumer errors: transient ones librdkafka retries itself, plus the
    /// error that ended the Source.
    source_errors,
    /// Stops whose final commit did not finish within `stop_timeout`.
    source_stop_timeouts,
    /// Records acknowledged by the broker (delivery report OK).
    sink_acked,
    /// Records whose delivery failed (timeout, broker error, purge).
    sink_failed,
    /// Rows over `max_message_bytes` / encode errors (batch not acked).
    sink_dropped_oversize,
    sink_dropped_bad,
    /// Waits because the producer queue or in-flight bound was full.
    sink_queue_full_waits,
    /// Rows of batches discarded at stop or after a fail-closed error.
    sink_discarded_on_close,
    /// The Sink failed closed (unconfirmed delivery, denied broker, missing
    /// topic); the supervisor fails the job.
    sink_fatal,
}

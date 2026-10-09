use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Process-wide I/O counters for ack/diagnostics. Live path only — not a
/// checkpoint or delivery receipt.
#[derive(Debug, Default)]
pub struct IoDiagnostics {
    pub lookup_update_failed: AtomicU64,
    pub plugin_source_rows: AtomicU64,
    pub plugin_sink_rows: AtomicU64,
    pub plugin_polls: AtomicU64,
    pub plugin_failed: AtomicU64,
    pub observation: Arc<sparrow_model::observation::FlowObservation>,
    pub source_queue: OnceLock<Arc<sparrow_io::observed::QueueObserver>>,
    pub sink_queue: OnceLock<Arc<sparrow_io::observed::QueueObserver>>,
    pub mqtt_received: AtomicU64,
    pub mqtt_inbox: Arc<sparrow_model::QueueOccupancy>,
    pub mqtt_inbox_metadata_bytes: AtomicU64,
    pub mqtt_pending_bytes: AtomicU64,
    pub mqtt_dropped_budget: AtomicU64,
    pub mqtt_dropped_oversize: AtomicU64,
    pub mqtt_accounted_sources: AtomicU64,
    pub mqtt_decoded: AtomicU64,
    pub mqtt_dropped_bad: AtomicU64,
    pub mqtt_dropped_full: AtomicU64,
    pub mqtt_backpressure_waits: AtomicU64,
    pub mqtt_backpressure_recovered: AtomicU64,
    pub mqtt_reconnects: AtomicU64,
    pub mqtt_feed_probes: AtomicU64,
    pub mqtt_feed_breaks: AtomicU64,
    pub mqtt_ping_timeouts: AtomicU64,
    pub mqtt_retained_ignored: AtomicU64,
    pub mqtt_quickack_calls: AtomicU64,
    pub mqtt_quickack_errors: AtomicU64,
    pub http_posted: AtomicU64,
    pub http_acked_batches: AtomicU64,
    pub http_encode_errors: AtomicU64,
    pub http_budget_drops: AtomicU64,
    pub http_failed: AtomicU64,
    pub http_dropped: AtomicU64,
    pub http_retries: AtomicU64,
    pub http_inflight: AtomicU64,
    pub http_poll_requests: AtomicU64,
    pub http_poll_ok: AtomicU64,
    pub http_poll_not_modified: AtomicU64,
    pub http_poll_failed: AtomicU64,
    pub http_poll_timeouts: AtomicU64,
    pub http_poll_status_errors: AtomicU64,
    pub http_poll_oversize: AtomicU64,
    pub http_poll_bad_responses: AtomicU64,
    pub http_poll_rows: AtomicU64,
    pub http_poll_dropped_bad: AtomicU64,
    pub http_poll_dropped_oversize: AtomicU64,
    pub http_poll_dropped_budget: AtomicU64,
    pub http_poll_skipped_ticks: AtomicU64,
    pub http_poll_backpressure_waits: AtomicU64,
    pub http_poll_inflight: AtomicU64,
    /// Admitted HTTP poll rows still in the inbox (byte-accounted).
    pub http_poll_inbox: Arc<sparrow_model::QueueOccupancy>,
    /// NATS Core (non-JetStream). Source counters are prefixed `nats_source_`,
    /// sink counters `nats_sink_`. `slow_consumer` counts SDK slow-consumer
    /// events: messages the client dropped because its bounded subscription
    /// buffer was full (a lower bound; the SDK event queue is lossy).
    pub nats_source_received: AtomicU64,
    pub nats_source_rows: AtomicU64,
    pub nats_source_dropped_bad: AtomicU64,
    pub nats_source_dropped_oversize: AtomicU64,
    pub nats_source_dropped_budget: AtomicU64,
    pub nats_source_backpressure_waits: AtomicU64,
    pub nats_source_slow_consumer: AtomicU64,
    pub nats_source_disconnects: AtomicU64,
    pub nats_source_reconnects: AtomicU64,
    pub nats_source_client_errors: AtomicU64,
    pub nats_sink_published: AtomicU64,
    pub nats_sink_failed: AtomicU64,
    pub nats_sink_dropped_bad: AtomicU64,
    pub nats_sink_dropped_oversize: AtomicU64,
    pub nats_sink_discarded_on_close: AtomicU64,
    pub nats_sink_flushes: AtomicU64,
    pub nats_sink_flush_failed: AtomicU64,
    pub nats_sink_disconnects: AtomicU64,
    pub nats_sink_reconnects: AtomicU64,
    pub nats_sink_client_errors: AtomicU64,
    pub nats_sink_sessions: AtomicU64,
    pub jetstream_sink_acked: AtomicU64,
    pub jetstream_sink_duplicates: AtomicU64,
    pub jetstream_sink_retries: AtomicU64,
    pub jetstream_sink_ack_timeouts: AtomicU64,
    pub jetstream_sink_failed: AtomicU64,
    pub jetstream_sink_batches: AtomicU64,
    pub jetstream_sink_discarded_on_close: AtomicU64,
    pub jetstream_sink_dropped_bad: AtomicU64,
    pub jetstream_sink_dropped_oversize: AtomicU64,
    pub jetstream_sink_msg_id_missing: AtomicU64,
    pub jetstream_sink_disconnects: AtomicU64,
    pub jetstream_sink_reconnects: AtomicU64,
    pub jetstream_sink_client_errors: AtomicU64,
    pub jetstream_sink_sessions: AtomicU64,
    pub jetstream_sink_fatal: AtomicU64,
    pub jetstream_sink_inflight: AtomicU64,
    pub databus_source_received: AtomicU64,
    pub databus_source_rows: AtomicU64,
    pub databus_source_dropped_bad: AtomicU64,
    pub databus_source_dropped_oversize: AtomicU64,
    pub databus_source_dropped_budget: AtomicU64,
    pub databus_source_dropped_oldest: AtomicU64,
    pub databus_source_dropped_newest: AtomicU64,
    pub databus_source_block_timeouts: AtomicU64,
    pub databus_source_backpressure_waits: AtomicU64,
    pub databus_source_discarded_on_close: AtomicU64,
    pub databus_source_buffer_items: AtomicU64,
    pub databus_source_buffer_bytes: AtomicU64,
    pub databus_source_subscriptions: AtomicU64,
    pub databus_sink_published: AtomicU64,
    pub databus_sink_deliveries: AtomicU64,
    pub databus_sink_no_subscribers: AtomicU64,
    pub databus_sink_dropped_bad: AtomicU64,
    pub databus_sink_dropped_oversize: AtomicU64,
    pub databus_sink_blocked_publishes: AtomicU64,
    pub databus_sink_discarded_on_close: AtomicU64,
    pub databus_sink_batches: AtomicU64,
    pub databus_sink_fatal: AtomicU64,
    pub csv_malformed: AtomicU64,
    pub csv_oversize: AtomicU64,
    pub csv_type_errors: AtomicU64,
    pub csv_header_errors: AtomicU64,
    pub csv_encode_errors: AtomicU64,
    pub websocket_source_received: AtomicU64,
    pub websocket_source_rows: AtomicU64,
    pub websocket_source_dropped_bad: AtomicU64,
    pub websocket_source_dropped_oversize: AtomicU64,
    pub websocket_source_dropped_binary: AtomicU64,
    pub websocket_source_dropped_budget: AtomicU64,
    pub websocket_source_backpressure_waits: AtomicU64,
    pub websocket_source_connects: AtomicU64,
    pub websocket_source_reconnects: AtomicU64,
    pub websocket_source_disconnects: AtomicU64,
    pub websocket_source_connect_failures: AtomicU64,
    pub websocket_source_heartbeat_timeouts: AtomicU64,
    pub websocket_source_pings_sent: AtomicU64,
    pub websocket_sink_sent: AtomicU64,
    pub websocket_sink_dropped_bad: AtomicU64,
    pub websocket_sink_dropped_oversize: AtomicU64,
    pub websocket_sink_dropped_overflow: AtomicU64,
    pub websocket_sink_backpressure_waits: AtomicU64,
    pub websocket_sink_send_failed: AtomicU64,
    pub websocket_sink_send_timeouts: AtomicU64,
    pub websocket_sink_discarded_on_close: AtomicU64,
    pub websocket_sink_connects: AtomicU64,
    pub websocket_sink_reconnects: AtomicU64,
    pub websocket_sink_disconnects: AtomicU64,
    pub websocket_sink_connect_failures: AtomicU64,
    pub websocket_sink_heartbeat_timeouts: AtomicU64,
    pub websocket_sink_pings_sent: AtomicU64,
    pub websocket_sink_ignored_frames: AtomicU64,
    pub websocket_sink_closes: AtomicU64,
    pub websocket_sink_close_failed: AtomicU64,
    pub websocket_sink_fatal: AtomicU64,
    pub websocket_sink_queue_items: AtomicU64,
    pub websocket_source_inbox: Arc<sparrow_model::QueueOccupancy>,
    pub databus_source_inbox: Arc<sparrow_model::QueueOccupancy>,
    pub nats_source_inbox: Arc<sparrow_model::QueueOccupancy>,
    pub log_written: AtomicU64,
    pub decode_errors: AtomicU64,
    pub file_written: AtomicU64,
    pub file_bytes: AtomicU64,
    pub file_segments: AtomicU64,
    pub file_syncs: AtomicU64,
    pub file_failed: AtomicU64,
}

impl IoDiagnostics {
    pub fn observe_source<T>(&self, tx: &sparrow_io::observed::Sender<T>) { if let Some(q)=tx.observer(){let _=self.source_queue.set(q);} }
    pub fn observe_sink<T>(&self, tx: &sparrow_io::observed::Sender<T>) { if let Some(q)=tx.observer(){let _=self.sink_queue.set(q);} }
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Count one CSV decode failure by kind (oversize, malformed, type,
    /// header). JSON failures are not counted here. The connector's own
    /// `*_dropped_bad` / `decode_errors` accounting is unchanged.
    pub fn csv_decode_error(
        &self,
        format: &sparrow_formats::PayloadFormat,
        error: &sparrow_model::SparrowError,
    ) {
        if format.as_csv().is_none() {
            return;
        }
        let counter = match sparrow_formats::CsvFault::of(error) {
            sparrow_formats::CsvFault::Oversize => &self.csv_oversize,
            sparrow_formats::CsvFault::Malformed => &self.csv_malformed,
            sparrow_formats::CsvFault::Type => &self.csv_type_errors,
            sparrow_formats::CsvFault::Header => &self.csv_header_errors,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one CSV encode failure (sinks).
    pub fn csv_encode_error(&self, format: &sparrow_formats::PayloadFormat) {
        if format.as_csv().is_some() {
            self.csv_encode_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            lookup_update_failed: self.lookup_update_failed.load(Ordering::Relaxed),
            plugin_source_rows: self.plugin_source_rows.load(Ordering::Relaxed),
            plugin_sink_rows: self.plugin_sink_rows.load(Ordering::Relaxed),
            plugin_polls: self.plugin_polls.load(Ordering::Relaxed),
            plugin_failed: self.plugin_failed.load(Ordering::Relaxed),
            mqtt_received: self.mqtt_received.load(Ordering::Relaxed),
            mqtt_inbox_metadata_bytes: self.mqtt_inbox_metadata_bytes.load(Ordering::Relaxed),
            mqtt_pending_bytes: self.mqtt_pending_bytes.load(Ordering::Relaxed),
            mqtt_dropped_budget: self.mqtt_dropped_budget.load(Ordering::Relaxed),
            mqtt_dropped_oversize: self.mqtt_dropped_oversize.load(Ordering::Relaxed),
            mqtt_accounted_sources: self.mqtt_accounted_sources.load(Ordering::Relaxed),
            mqtt_inbox_items: self.mqtt_inbox.items.load(Ordering::Relaxed),
            mqtt_inbox_bytes: self.mqtt_inbox.bytes.load(Ordering::Relaxed),
            mqtt_inbox_peak_items: self.mqtt_inbox.peak_items.load(Ordering::Relaxed),
            mqtt_inbox_peak_bytes: self.mqtt_inbox.peak_bytes.load(Ordering::Relaxed),
            mqtt_decoded: self.mqtt_decoded.load(Ordering::Relaxed),
            mqtt_dropped_bad: self.mqtt_dropped_bad.load(Ordering::Relaxed),
            mqtt_dropped_full: self.mqtt_dropped_full.load(Ordering::Relaxed),
            mqtt_backpressure_waits: self.mqtt_backpressure_waits.load(Ordering::Relaxed),
            mqtt_backpressure_recovered: self.mqtt_backpressure_recovered.load(Ordering::Relaxed),
            mqtt_reconnects: self.mqtt_reconnects.load(Ordering::Relaxed),
            mqtt_feed_probes: self.mqtt_feed_probes.load(Ordering::Relaxed),
            mqtt_feed_breaks: self.mqtt_feed_breaks.load(Ordering::Relaxed),
            mqtt_ping_timeouts: self.mqtt_ping_timeouts.load(Ordering::Relaxed),
            mqtt_retained_ignored: self.mqtt_retained_ignored.load(Ordering::Relaxed),
            mqtt_quickack_calls: self.mqtt_quickack_calls.load(Ordering::Relaxed),
            mqtt_quickack_errors: self.mqtt_quickack_errors.load(Ordering::Relaxed),
            http_posted: self.http_posted.load(Ordering::Relaxed),
            http_acked_batches: self.http_acked_batches.load(Ordering::Relaxed),
            http_encode_errors: self.http_encode_errors.load(Ordering::Relaxed),
            http_budget_drops: self.http_budget_drops.load(Ordering::Relaxed),
            http_failed: self.http_failed.load(Ordering::Relaxed),
            http_dropped: self.http_dropped.load(Ordering::Relaxed),
            http_retries: self.http_retries.load(Ordering::Relaxed),
            http_inflight: self.http_inflight.load(Ordering::Relaxed),
            http_poll_requests: self.http_poll_requests.load(Ordering::Relaxed),
            http_poll_ok: self.http_poll_ok.load(Ordering::Relaxed),
            http_poll_not_modified: self.http_poll_not_modified.load(Ordering::Relaxed),
            http_poll_failed: self.http_poll_failed.load(Ordering::Relaxed),
            http_poll_timeouts: self.http_poll_timeouts.load(Ordering::Relaxed),
            http_poll_status_errors: self.http_poll_status_errors.load(Ordering::Relaxed),
            http_poll_oversize: self.http_poll_oversize.load(Ordering::Relaxed),
            http_poll_bad_responses: self.http_poll_bad_responses.load(Ordering::Relaxed),
            http_poll_rows: self.http_poll_rows.load(Ordering::Relaxed),
            http_poll_dropped_bad: self.http_poll_dropped_bad.load(Ordering::Relaxed),
            http_poll_dropped_oversize: self.http_poll_dropped_oversize.load(Ordering::Relaxed),
            http_poll_dropped_budget: self.http_poll_dropped_budget.load(Ordering::Relaxed),
            http_poll_skipped_ticks: self.http_poll_skipped_ticks.load(Ordering::Relaxed),
            http_poll_backpressure_waits: self.http_poll_backpressure_waits.load(Ordering::Relaxed),
            http_poll_inflight: self.http_poll_inflight.load(Ordering::Relaxed),
            http_poll_inbox_items: self.http_poll_inbox.items.load(Ordering::Relaxed),
            http_poll_inbox_bytes: self.http_poll_inbox.bytes.load(Ordering::Relaxed),
            nats_source_received: self.nats_source_received.load(Ordering::Relaxed),
            nats_source_rows: self.nats_source_rows.load(Ordering::Relaxed),
            nats_source_dropped_bad: self.nats_source_dropped_bad.load(Ordering::Relaxed),
            nats_source_dropped_oversize: self.nats_source_dropped_oversize.load(Ordering::Relaxed),
            nats_source_dropped_budget: self.nats_source_dropped_budget.load(Ordering::Relaxed),
            nats_source_backpressure_waits: self
                .nats_source_backpressure_waits
                .load(Ordering::Relaxed),
            nats_source_slow_consumer: self.nats_source_slow_consumer.load(Ordering::Relaxed),
            nats_source_disconnects: self.nats_source_disconnects.load(Ordering::Relaxed),
            nats_source_reconnects: self.nats_source_reconnects.load(Ordering::Relaxed),
            nats_source_client_errors: self.nats_source_client_errors.load(Ordering::Relaxed),
            nats_sink_published: self.nats_sink_published.load(Ordering::Relaxed),
            nats_sink_failed: self.nats_sink_failed.load(Ordering::Relaxed),
            nats_sink_dropped_bad: self.nats_sink_dropped_bad.load(Ordering::Relaxed),
            nats_sink_dropped_oversize: self.nats_sink_dropped_oversize.load(Ordering::Relaxed),
            nats_sink_discarded_on_close: self.nats_sink_discarded_on_close.load(Ordering::Relaxed),
            nats_sink_flushes: self.nats_sink_flushes.load(Ordering::Relaxed),
            nats_sink_flush_failed: self.nats_sink_flush_failed.load(Ordering::Relaxed),
            nats_sink_disconnects: self.nats_sink_disconnects.load(Ordering::Relaxed),
            nats_sink_reconnects: self.nats_sink_reconnects.load(Ordering::Relaxed),
            nats_sink_client_errors: self.nats_sink_client_errors.load(Ordering::Relaxed),
            nats_sink_sessions: self.nats_sink_sessions.load(Ordering::Relaxed),
            jetstream_sink_acked: self.jetstream_sink_acked.load(Ordering::Relaxed),
            jetstream_sink_duplicates: self.jetstream_sink_duplicates.load(Ordering::Relaxed),
            jetstream_sink_retries: self.jetstream_sink_retries.load(Ordering::Relaxed),
            jetstream_sink_ack_timeouts: self.jetstream_sink_ack_timeouts.load(Ordering::Relaxed),
            jetstream_sink_failed: self.jetstream_sink_failed.load(Ordering::Relaxed),
            jetstream_sink_batches: self.jetstream_sink_batches.load(Ordering::Relaxed),
            jetstream_sink_discarded_on_close: self
                .jetstream_sink_discarded_on_close
                .load(Ordering::Relaxed),
            jetstream_sink_dropped_bad: self.jetstream_sink_dropped_bad.load(Ordering::Relaxed),
            jetstream_sink_dropped_oversize: self
                .jetstream_sink_dropped_oversize
                .load(Ordering::Relaxed),
            jetstream_sink_msg_id_missing: self
                .jetstream_sink_msg_id_missing
                .load(Ordering::Relaxed),
            jetstream_sink_disconnects: self.jetstream_sink_disconnects.load(Ordering::Relaxed),
            jetstream_sink_reconnects: self.jetstream_sink_reconnects.load(Ordering::Relaxed),
            jetstream_sink_client_errors: self.jetstream_sink_client_errors.load(Ordering::Relaxed),
            jetstream_sink_sessions: self.jetstream_sink_sessions.load(Ordering::Relaxed),
            jetstream_sink_fatal: self.jetstream_sink_fatal.load(Ordering::Relaxed),
            jetstream_sink_inflight: self.jetstream_sink_inflight.load(Ordering::Relaxed),
            databus_source_received: self.databus_source_received.load(Ordering::Relaxed),
            databus_source_rows: self.databus_source_rows.load(Ordering::Relaxed),
            databus_source_dropped_bad: self.databus_source_dropped_bad.load(Ordering::Relaxed),
            databus_source_dropped_oversize: self
                .databus_source_dropped_oversize
                .load(Ordering::Relaxed),
            databus_source_dropped_budget: self
                .databus_source_dropped_budget
                .load(Ordering::Relaxed),
            databus_source_dropped_oldest: self
                .databus_source_dropped_oldest
                .load(Ordering::Relaxed),
            databus_source_dropped_newest: self
                .databus_source_dropped_newest
                .load(Ordering::Relaxed),
            databus_source_block_timeouts: self
                .databus_source_block_timeouts
                .load(Ordering::Relaxed),
            databus_source_backpressure_waits: self
                .databus_source_backpressure_waits
                .load(Ordering::Relaxed),
            databus_source_discarded_on_close: self
                .databus_source_discarded_on_close
                .load(Ordering::Relaxed),
            databus_source_buffer_items: self.databus_source_buffer_items.load(Ordering::Relaxed),
            databus_source_buffer_bytes: self.databus_source_buffer_bytes.load(Ordering::Relaxed),
            databus_source_subscriptions: self.databus_source_subscriptions.load(Ordering::Relaxed),
            databus_sink_published: self.databus_sink_published.load(Ordering::Relaxed),
            databus_sink_deliveries: self.databus_sink_deliveries.load(Ordering::Relaxed),
            databus_sink_no_subscribers: self.databus_sink_no_subscribers.load(Ordering::Relaxed),
            databus_sink_dropped_bad: self.databus_sink_dropped_bad.load(Ordering::Relaxed),
            databus_sink_dropped_oversize: self
                .databus_sink_dropped_oversize
                .load(Ordering::Relaxed),
            databus_sink_blocked_publishes: self
                .databus_sink_blocked_publishes
                .load(Ordering::Relaxed),
            databus_sink_discarded_on_close: self
                .databus_sink_discarded_on_close
                .load(Ordering::Relaxed),
            databus_sink_batches: self.databus_sink_batches.load(Ordering::Relaxed),
            databus_sink_fatal: self.databus_sink_fatal.load(Ordering::Relaxed),
            csv_malformed: self.csv_malformed.load(Ordering::Relaxed),
            csv_oversize: self.csv_oversize.load(Ordering::Relaxed),
            csv_type_errors: self.csv_type_errors.load(Ordering::Relaxed),
            csv_header_errors: self.csv_header_errors.load(Ordering::Relaxed),
            csv_encode_errors: self.csv_encode_errors.load(Ordering::Relaxed),
            websocket_source_received: self.websocket_source_received.load(Ordering::Relaxed),
            websocket_source_rows: self.websocket_source_rows.load(Ordering::Relaxed),
            websocket_source_dropped_bad: self.websocket_source_dropped_bad.load(Ordering::Relaxed),
            websocket_source_dropped_oversize: self
                .websocket_source_dropped_oversize
                .load(Ordering::Relaxed),
            websocket_source_dropped_binary: self
                .websocket_source_dropped_binary
                .load(Ordering::Relaxed),
            websocket_source_dropped_budget: self
                .websocket_source_dropped_budget
                .load(Ordering::Relaxed),
            websocket_source_backpressure_waits: self
                .websocket_source_backpressure_waits
                .load(Ordering::Relaxed),
            websocket_source_connects: self.websocket_source_connects.load(Ordering::Relaxed),
            websocket_source_reconnects: self.websocket_source_reconnects.load(Ordering::Relaxed),
            websocket_source_disconnects: self.websocket_source_disconnects.load(Ordering::Relaxed),
            websocket_source_connect_failures: self
                .websocket_source_connect_failures
                .load(Ordering::Relaxed),
            websocket_source_heartbeat_timeouts: self
                .websocket_source_heartbeat_timeouts
                .load(Ordering::Relaxed),
            websocket_source_pings_sent: self.websocket_source_pings_sent.load(Ordering::Relaxed),
            websocket_sink_sent: self.websocket_sink_sent.load(Ordering::Relaxed),
            websocket_sink_dropped_bad: self.websocket_sink_dropped_bad.load(Ordering::Relaxed),
            websocket_sink_dropped_oversize: self
                .websocket_sink_dropped_oversize
                .load(Ordering::Relaxed),
            websocket_sink_dropped_overflow: self
                .websocket_sink_dropped_overflow
                .load(Ordering::Relaxed),
            websocket_sink_backpressure_waits: self
                .websocket_sink_backpressure_waits
                .load(Ordering::Relaxed),
            websocket_sink_send_failed: self.websocket_sink_send_failed.load(Ordering::Relaxed),
            websocket_sink_send_timeouts: self.websocket_sink_send_timeouts.load(Ordering::Relaxed),
            websocket_sink_discarded_on_close: self
                .websocket_sink_discarded_on_close
                .load(Ordering::Relaxed),
            websocket_sink_connects: self.websocket_sink_connects.load(Ordering::Relaxed),
            websocket_sink_reconnects: self.websocket_sink_reconnects.load(Ordering::Relaxed),
            websocket_sink_disconnects: self.websocket_sink_disconnects.load(Ordering::Relaxed),
            websocket_sink_connect_failures: self
                .websocket_sink_connect_failures
                .load(Ordering::Relaxed),
            websocket_sink_heartbeat_timeouts: self
                .websocket_sink_heartbeat_timeouts
                .load(Ordering::Relaxed),
            websocket_sink_pings_sent: self.websocket_sink_pings_sent.load(Ordering::Relaxed),
            websocket_sink_ignored_frames: self
                .websocket_sink_ignored_frames
                .load(Ordering::Relaxed),
            websocket_sink_closes: self.websocket_sink_closes.load(Ordering::Relaxed),
            websocket_sink_close_failed: self.websocket_sink_close_failed.load(Ordering::Relaxed),
            websocket_sink_fatal: self.websocket_sink_fatal.load(Ordering::Relaxed),
            websocket_sink_queue_items: self.websocket_sink_queue_items.load(Ordering::Relaxed),
            websocket_source_inbox_items: self.websocket_source_inbox.items.load(Ordering::Relaxed),
            websocket_source_inbox_bytes: self.websocket_source_inbox.bytes.load(Ordering::Relaxed),
            databus_source_inbox_items: self.databus_source_inbox.items.load(Ordering::Relaxed),
            databus_source_inbox_bytes: self.databus_source_inbox.bytes.load(Ordering::Relaxed),
            nats_source_inbox_items: self.nats_source_inbox.items.load(Ordering::Relaxed),
            nats_source_inbox_bytes: self.nats_source_inbox.bytes.load(Ordering::Relaxed),
            log_written: self.log_written.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            file_written: self.file_written.load(Ordering::Relaxed),
            file_bytes: self.file_bytes.load(Ordering::Relaxed),
            file_segments: self.file_segments.load(Ordering::Relaxed),
            file_syncs: self.file_syncs.load(Ordering::Relaxed),
            file_failed: self.file_failed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IoSnapshot {
    pub lookup_update_failed: u64,
    pub plugin_source_rows: u64,
    pub plugin_sink_rows: u64,
    pub plugin_polls: u64,
    pub plugin_failed: u64,
    pub mqtt_received: u64,
    pub mqtt_inbox_metadata_bytes: u64,
    pub mqtt_pending_bytes: u64,
    pub mqtt_dropped_budget: u64,
    pub mqtt_dropped_oversize: u64,
    pub mqtt_accounted_sources: u64,
    pub mqtt_inbox_items: u64,
    pub mqtt_inbox_bytes: u64,
    pub mqtt_inbox_peak_items: u64,
    pub mqtt_inbox_peak_bytes: u64,
    pub mqtt_decoded: u64,
    pub mqtt_dropped_bad: u64,
    pub mqtt_dropped_full: u64,
    pub mqtt_backpressure_waits: u64,
    pub mqtt_backpressure_recovered: u64,
    pub mqtt_reconnects: u64,
    pub mqtt_feed_probes: u64,
    pub mqtt_feed_breaks: u64,
    pub mqtt_ping_timeouts: u64,
    pub mqtt_retained_ignored: u64,
    pub mqtt_quickack_calls: u64,
    pub mqtt_quickack_errors: u64,
    pub http_posted: u64,
    pub http_acked_batches: u64,
    pub http_encode_errors: u64,
    pub http_budget_drops: u64,
    pub http_failed: u64,
    pub http_dropped: u64,
    pub http_retries: u64,
    pub http_inflight: u64,
    pub http_poll_requests: u64,
    pub http_poll_ok: u64,
    pub http_poll_not_modified: u64,
    pub http_poll_failed: u64,
    pub http_poll_timeouts: u64,
    pub http_poll_status_errors: u64,
    pub http_poll_oversize: u64,
    pub http_poll_bad_responses: u64,
    pub http_poll_rows: u64,
    pub http_poll_dropped_bad: u64,
    pub http_poll_dropped_oversize: u64,
    pub http_poll_dropped_budget: u64,
    pub http_poll_skipped_ticks: u64,
    pub http_poll_backpressure_waits: u64,
    pub http_poll_inflight: u64,
    pub http_poll_inbox_items: u64,
    pub http_poll_inbox_bytes: u64,
    pub nats_source_received: u64,
    pub nats_source_rows: u64,
    pub nats_source_dropped_bad: u64,
    pub nats_source_dropped_oversize: u64,
    pub nats_source_dropped_budget: u64,
    pub nats_source_backpressure_waits: u64,
    pub nats_source_slow_consumer: u64,
    pub nats_source_disconnects: u64,
    pub nats_source_reconnects: u64,
    pub nats_source_client_errors: u64,
    pub nats_sink_published: u64,
    pub nats_sink_failed: u64,
    pub nats_sink_dropped_bad: u64,
    pub nats_sink_dropped_oversize: u64,
    pub nats_sink_discarded_on_close: u64,
    pub nats_sink_flushes: u64,
    pub nats_sink_flush_failed: u64,
    pub nats_sink_disconnects: u64,
    pub nats_sink_reconnects: u64,
    pub nats_sink_client_errors: u64,
    pub nats_sink_sessions: u64,
    pub jetstream_sink_acked: u64,
    pub jetstream_sink_duplicates: u64,
    pub jetstream_sink_retries: u64,
    pub jetstream_sink_ack_timeouts: u64,
    pub jetstream_sink_failed: u64,
    pub jetstream_sink_batches: u64,
    pub jetstream_sink_discarded_on_close: u64,
    pub jetstream_sink_dropped_bad: u64,
    pub jetstream_sink_dropped_oversize: u64,
    pub jetstream_sink_msg_id_missing: u64,
    pub jetstream_sink_disconnects: u64,
    pub jetstream_sink_reconnects: u64,
    pub jetstream_sink_client_errors: u64,
    pub jetstream_sink_sessions: u64,
    pub jetstream_sink_fatal: u64,
    pub jetstream_sink_inflight: u64,
    pub databus_source_received: u64,
    pub databus_source_rows: u64,
    pub databus_source_dropped_bad: u64,
    pub databus_source_dropped_oversize: u64,
    pub databus_source_dropped_budget: u64,
    pub databus_source_dropped_oldest: u64,
    pub databus_source_dropped_newest: u64,
    pub databus_source_block_timeouts: u64,
    pub databus_source_backpressure_waits: u64,
    pub databus_source_discarded_on_close: u64,
    pub databus_source_buffer_items: u64,
    pub databus_source_buffer_bytes: u64,
    pub databus_source_subscriptions: u64,
    pub databus_sink_published: u64,
    pub databus_sink_deliveries: u64,
    pub databus_sink_no_subscribers: u64,
    pub databus_sink_dropped_bad: u64,
    pub databus_sink_dropped_oversize: u64,
    pub databus_sink_blocked_publishes: u64,
    pub databus_sink_discarded_on_close: u64,
    pub databus_sink_batches: u64,
    pub databus_sink_fatal: u64,
    pub csv_malformed: u64,
    pub csv_oversize: u64,
    pub csv_type_errors: u64,
    pub csv_header_errors: u64,
    pub csv_encode_errors: u64,
    pub websocket_source_received: u64,
    pub websocket_source_rows: u64,
    pub websocket_source_dropped_bad: u64,
    pub websocket_source_dropped_oversize: u64,
    pub websocket_source_dropped_binary: u64,
    pub websocket_source_dropped_budget: u64,
    pub websocket_source_backpressure_waits: u64,
    pub websocket_source_connects: u64,
    pub websocket_source_reconnects: u64,
    pub websocket_source_disconnects: u64,
    pub websocket_source_connect_failures: u64,
    pub websocket_source_heartbeat_timeouts: u64,
    pub websocket_source_pings_sent: u64,
    pub websocket_sink_sent: u64,
    pub websocket_sink_dropped_bad: u64,
    pub websocket_sink_dropped_oversize: u64,
    pub websocket_sink_dropped_overflow: u64,
    pub websocket_sink_backpressure_waits: u64,
    pub websocket_sink_send_failed: u64,
    pub websocket_sink_send_timeouts: u64,
    pub websocket_sink_discarded_on_close: u64,
    pub websocket_sink_connects: u64,
    pub websocket_sink_reconnects: u64,
    pub websocket_sink_disconnects: u64,
    pub websocket_sink_connect_failures: u64,
    pub websocket_sink_heartbeat_timeouts: u64,
    pub websocket_sink_pings_sent: u64,
    pub websocket_sink_ignored_frames: u64,
    pub websocket_sink_closes: u64,
    pub websocket_sink_close_failed: u64,
    pub websocket_sink_fatal: u64,
    pub websocket_sink_queue_items: u64,
    pub websocket_source_inbox_items: u64,
    pub websocket_source_inbox_bytes: u64,
    pub databus_source_inbox_items: u64,
    pub databus_source_inbox_bytes: u64,
    pub nats_source_inbox_items: u64,
    pub nats_source_inbox_bytes: u64,
    pub log_written: u64,
    pub decode_errors: u64,
    pub file_written: u64,
    pub file_bytes: u64,
    pub file_segments: u64,
    pub file_syncs: u64,
    pub file_failed: u64,
}

impl std::fmt::Display for IoSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mqtt recv={} decoded={} bad={} full={} waits={} recovered={} reconnects={} | http posted={} failed={} dropped={} retries={} inflight={} | log={} decode_err={}",
            self.mqtt_received,
            self.mqtt_decoded,
            self.mqtt_dropped_bad,
            self.mqtt_dropped_full,
            self.mqtt_backpressure_waits,
            self.mqtt_backpressure_recovered,
            self.mqtt_reconnects,
            self.http_posted,
            self.http_failed,
            self.http_dropped,
            self.http_retries,
            self.http_inflight,
            self.log_written,
            self.decode_errors
        )
    }
}

impl IoSnapshot {
    pub fn add_assign(&mut self, other: &IoSnapshot) {
        self.lookup_update_failed += other.lookup_update_failed;
        self.plugin_source_rows += other.plugin_source_rows;
        self.plugin_sink_rows += other.plugin_sink_rows;
        self.plugin_polls += other.plugin_polls;
        self.plugin_failed += other.plugin_failed;
        self.mqtt_received += other.mqtt_received;
        self.mqtt_inbox_metadata_bytes += other.mqtt_inbox_metadata_bytes;
        self.mqtt_pending_bytes += other.mqtt_pending_bytes;
        self.mqtt_dropped_budget += other.mqtt_dropped_budget;
        self.mqtt_dropped_oversize += other.mqtt_dropped_oversize;
        self.mqtt_accounted_sources += other.mqtt_accounted_sources;
        self.mqtt_inbox_items += other.mqtt_inbox_items;
        self.mqtt_inbox_bytes += other.mqtt_inbox_bytes;
        self.mqtt_inbox_peak_items += other.mqtt_inbox_peak_items;
        self.mqtt_inbox_peak_bytes += other.mqtt_inbox_peak_bytes;
        self.mqtt_decoded += other.mqtt_decoded;
        self.mqtt_dropped_bad += other.mqtt_dropped_bad;
        self.mqtt_dropped_full += other.mqtt_dropped_full;
        self.mqtt_backpressure_waits += other.mqtt_backpressure_waits;
        self.mqtt_backpressure_recovered += other.mqtt_backpressure_recovered;
        self.mqtt_reconnects += other.mqtt_reconnects;
        self.mqtt_feed_probes += other.mqtt_feed_probes;
        self.mqtt_feed_breaks += other.mqtt_feed_breaks;
        self.mqtt_ping_timeouts += other.mqtt_ping_timeouts;
        self.mqtt_retained_ignored += other.mqtt_retained_ignored;
        self.mqtt_quickack_calls += other.mqtt_quickack_calls;
        self.mqtt_quickack_errors += other.mqtt_quickack_errors;
        self.http_posted += other.http_posted;
        self.http_acked_batches += other.http_acked_batches;
        self.http_encode_errors += other.http_encode_errors;
        self.http_budget_drops += other.http_budget_drops;
        self.http_failed += other.http_failed;
        self.http_dropped += other.http_dropped;
        self.http_retries += other.http_retries;
        self.http_inflight += other.http_inflight;
        self.http_poll_requests += other.http_poll_requests;
        self.http_poll_ok += other.http_poll_ok;
        self.http_poll_not_modified += other.http_poll_not_modified;
        self.http_poll_failed += other.http_poll_failed;
        self.http_poll_timeouts += other.http_poll_timeouts;
        self.http_poll_status_errors += other.http_poll_status_errors;
        self.http_poll_oversize += other.http_poll_oversize;
        self.http_poll_bad_responses += other.http_poll_bad_responses;
        self.http_poll_rows += other.http_poll_rows;
        self.http_poll_dropped_bad += other.http_poll_dropped_bad;
        self.http_poll_dropped_oversize += other.http_poll_dropped_oversize;
        self.http_poll_dropped_budget += other.http_poll_dropped_budget;
        self.http_poll_skipped_ticks += other.http_poll_skipped_ticks;
        self.http_poll_backpressure_waits += other.http_poll_backpressure_waits;
        self.http_poll_inflight += other.http_poll_inflight;
        self.http_poll_inbox_items += other.http_poll_inbox_items;
        self.http_poll_inbox_bytes += other.http_poll_inbox_bytes;
        self.nats_source_received += other.nats_source_received;
        self.nats_source_rows += other.nats_source_rows;
        self.nats_source_dropped_bad += other.nats_source_dropped_bad;
        self.nats_source_dropped_oversize += other.nats_source_dropped_oversize;
        self.nats_source_dropped_budget += other.nats_source_dropped_budget;
        self.nats_source_backpressure_waits += other.nats_source_backpressure_waits;
        self.nats_source_slow_consumer += other.nats_source_slow_consumer;
        self.nats_source_disconnects += other.nats_source_disconnects;
        self.nats_source_reconnects += other.nats_source_reconnects;
        self.nats_source_client_errors += other.nats_source_client_errors;
        self.nats_sink_published += other.nats_sink_published;
        self.nats_sink_failed += other.nats_sink_failed;
        self.nats_sink_dropped_bad += other.nats_sink_dropped_bad;
        self.nats_sink_dropped_oversize += other.nats_sink_dropped_oversize;
        self.nats_sink_discarded_on_close += other.nats_sink_discarded_on_close;
        self.nats_sink_flushes += other.nats_sink_flushes;
        self.nats_sink_flush_failed += other.nats_sink_flush_failed;
        self.nats_sink_disconnects += other.nats_sink_disconnects;
        self.nats_sink_reconnects += other.nats_sink_reconnects;
        self.nats_sink_client_errors += other.nats_sink_client_errors;
        self.nats_sink_sessions += other.nats_sink_sessions;
        self.jetstream_sink_acked += other.jetstream_sink_acked;
        self.jetstream_sink_duplicates += other.jetstream_sink_duplicates;
        self.jetstream_sink_retries += other.jetstream_sink_retries;
        self.jetstream_sink_ack_timeouts += other.jetstream_sink_ack_timeouts;
        self.jetstream_sink_failed += other.jetstream_sink_failed;
        self.jetstream_sink_batches += other.jetstream_sink_batches;
        self.jetstream_sink_discarded_on_close += other.jetstream_sink_discarded_on_close;
        self.jetstream_sink_dropped_bad += other.jetstream_sink_dropped_bad;
        self.jetstream_sink_dropped_oversize += other.jetstream_sink_dropped_oversize;
        self.jetstream_sink_msg_id_missing += other.jetstream_sink_msg_id_missing;
        self.jetstream_sink_disconnects += other.jetstream_sink_disconnects;
        self.jetstream_sink_reconnects += other.jetstream_sink_reconnects;
        self.jetstream_sink_client_errors += other.jetstream_sink_client_errors;
        self.jetstream_sink_sessions += other.jetstream_sink_sessions;
        self.jetstream_sink_fatal += other.jetstream_sink_fatal;
        self.jetstream_sink_inflight += other.jetstream_sink_inflight;
        self.databus_source_received += other.databus_source_received;
        self.databus_source_rows += other.databus_source_rows;
        self.databus_source_dropped_bad += other.databus_source_dropped_bad;
        self.databus_source_dropped_oversize += other.databus_source_dropped_oversize;
        self.databus_source_dropped_budget += other.databus_source_dropped_budget;
        self.databus_source_dropped_oldest += other.databus_source_dropped_oldest;
        self.databus_source_dropped_newest += other.databus_source_dropped_newest;
        self.databus_source_block_timeouts += other.databus_source_block_timeouts;
        self.databus_source_backpressure_waits += other.databus_source_backpressure_waits;
        self.databus_source_discarded_on_close += other.databus_source_discarded_on_close;
        self.databus_source_buffer_items += other.databus_source_buffer_items;
        self.databus_source_buffer_bytes += other.databus_source_buffer_bytes;
        self.databus_source_subscriptions += other.databus_source_subscriptions;
        self.databus_sink_published += other.databus_sink_published;
        self.databus_sink_deliveries += other.databus_sink_deliveries;
        self.databus_sink_no_subscribers += other.databus_sink_no_subscribers;
        self.databus_sink_dropped_bad += other.databus_sink_dropped_bad;
        self.databus_sink_dropped_oversize += other.databus_sink_dropped_oversize;
        self.databus_sink_blocked_publishes += other.databus_sink_blocked_publishes;
        self.databus_sink_discarded_on_close += other.databus_sink_discarded_on_close;
        self.databus_sink_batches += other.databus_sink_batches;
        self.databus_sink_fatal += other.databus_sink_fatal;
        self.csv_malformed += other.csv_malformed;
        self.csv_oversize += other.csv_oversize;
        self.csv_type_errors += other.csv_type_errors;
        self.csv_header_errors += other.csv_header_errors;
        self.csv_encode_errors += other.csv_encode_errors;
        self.websocket_source_received += other.websocket_source_received;
        self.websocket_source_rows += other.websocket_source_rows;
        self.websocket_source_dropped_bad += other.websocket_source_dropped_bad;
        self.websocket_source_dropped_oversize += other.websocket_source_dropped_oversize;
        self.websocket_source_dropped_binary += other.websocket_source_dropped_binary;
        self.websocket_source_dropped_budget += other.websocket_source_dropped_budget;
        self.websocket_source_backpressure_waits += other.websocket_source_backpressure_waits;
        self.websocket_source_connects += other.websocket_source_connects;
        self.websocket_source_reconnects += other.websocket_source_reconnects;
        self.websocket_source_disconnects += other.websocket_source_disconnects;
        self.websocket_source_connect_failures += other.websocket_source_connect_failures;
        self.websocket_source_heartbeat_timeouts += other.websocket_source_heartbeat_timeouts;
        self.websocket_source_pings_sent += other.websocket_source_pings_sent;
        self.websocket_sink_sent += other.websocket_sink_sent;
        self.websocket_sink_dropped_bad += other.websocket_sink_dropped_bad;
        self.websocket_sink_dropped_oversize += other.websocket_sink_dropped_oversize;
        self.websocket_sink_dropped_overflow += other.websocket_sink_dropped_overflow;
        self.websocket_sink_backpressure_waits += other.websocket_sink_backpressure_waits;
        self.websocket_sink_send_failed += other.websocket_sink_send_failed;
        self.websocket_sink_send_timeouts += other.websocket_sink_send_timeouts;
        self.websocket_sink_discarded_on_close += other.websocket_sink_discarded_on_close;
        self.websocket_sink_connects += other.websocket_sink_connects;
        self.websocket_sink_reconnects += other.websocket_sink_reconnects;
        self.websocket_sink_disconnects += other.websocket_sink_disconnects;
        self.websocket_sink_connect_failures += other.websocket_sink_connect_failures;
        self.websocket_sink_heartbeat_timeouts += other.websocket_sink_heartbeat_timeouts;
        self.websocket_sink_pings_sent += other.websocket_sink_pings_sent;
        self.websocket_sink_ignored_frames += other.websocket_sink_ignored_frames;
        self.websocket_sink_closes += other.websocket_sink_closes;
        self.websocket_sink_close_failed += other.websocket_sink_close_failed;
        self.websocket_sink_fatal += other.websocket_sink_fatal;
        self.websocket_sink_queue_items += other.websocket_sink_queue_items;
        self.websocket_source_inbox_items += other.websocket_source_inbox_items;
        self.websocket_source_inbox_bytes += other.websocket_source_inbox_bytes;
        self.databus_source_inbox_items += other.databus_source_inbox_items;
        self.databus_source_inbox_bytes += other.databus_source_inbox_bytes;
        self.nats_source_inbox_items += other.nats_source_inbox_items;
        self.nats_source_inbox_bytes += other.nats_source_inbox_bytes;
        self.log_written += other.log_written;
        self.decode_errors += other.decode_errors;
        self.file_written += other.file_written;
        self.file_bytes += other.file_bytes;
        self.file_segments += other.file_segments;
        self.file_syncs += other.file_syncs;
        self.file_failed += other.file_failed;
    }
}

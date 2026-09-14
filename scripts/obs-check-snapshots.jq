# jq -s -f scripts/obs-check-snapshots.jq *-metrics-timeline.jsonl
# Checks per-component invariants only; never assumes an atomic pipeline cut.
def histogram_ok:
  (.buckets | length) == 32 and (.buckets | add) == .samples
  and (if .samples < 100 then .p99_upper_us == null else true end);
def queue_ok:
  .queued_items <= .capacity_items
  and .enqueued_total == (.received_total + .discarded_on_close_total + .queued_items)
  and .residence.samples == ((.received_total / (.residence_sample_every // 1)) | floor)
  and .capacity_wait.samples + .waiting_senders == .capacity_waits_total
  and (if .queued_items == 0 then .oldest_queued_age_us == null else .oldest_queued_age_us != null end)
  and (.residence | histogram_ok) and (.capacity_wait | histogram_ok);
def delivery_ok:
  .dequeued_batches_total == (.completed_batches_total + .failed_or_cancelled_batches_total + .active_input_batches)
  and .dequeued_rows_total == (.completed_rows_total + .failed_or_cancelled_rows_total + .active_rows)
  and .http_attempts_started_total == (.http_attempts_finished_total + .http_attempts_cancelled_total + .active_http_requests);
def mailbox_ok:
  .accounting_valid == true and .accounting_errors_total == 0
  and .queued.items <= .max_items and .credits_reserved_bytes <= .max_bytes
  and .enqueued_items_total == (.received_items_total + .discarded_on_close_total + .queued.items)
  and .queued.items == (.queued.data_batches + .queued.controls)
  and .consumer_held.items == (.consumer_held.data_batches + .consumer_held.controls)
  and (.residence | histogram_ok);
def view_ok:
  (.delivery | delivery_ok) and (.latency | to_entries | all(.value | histogram_ok));
[.[] | (if has("metrics") then .metrics else . end) | .observations.jobs[]? | .observation | select(.available)] as $views
| [$views[] | .source_inbox, .sink_outbox | select(.available)] as $queues
| [.[] | (if has("metrics") then .metrics else . end) | .mailboxes.jobs[]?.observation | select(.available) | .edges[]] as $mailboxes
| {
  observable_snapshots: ($views | length),
  queue_snapshots: ($queues | length),
  mailbox_snapshots: ($mailboxes | length),
  coherent: (($views | length) > 0 and ($queues | all(queue_ok)) and ($views | all(view_ok)) and ($mailboxes | all(mailbox_ok))),
  max_source_queue_items: ([$views[].source_inbox.queued_items] | max),
  max_sink_queue_items: ([$views[].sink_outbox.queued_items] | max),
  max_source_oldest_age_us: ([$views[].source_inbox.oldest_queued_age_us // 0] | max),
  max_sink_oldest_age_us: ([$views[].sink_outbox.oldest_queued_age_us // 0] | max),
  max_source_waits: ([$views[].source_inbox.capacity_waits_total] | max),
  max_sink_waits: ([$views[].sink_outbox.capacity_waits_total] | max),
  peak_encoded_credit_bytes: ([$views[].delivery.peak_encoded_credit_bytes] | max),
  peak_active_http_requests: ([$views[].delivery.active_http_requests] | max),
  note: "sampled maxima; per-component consistency, not a lossless/whole-pipeline receipt proof"
}

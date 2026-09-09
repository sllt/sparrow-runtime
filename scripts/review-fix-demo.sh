#!/usr/bin/env bash
# Process / API evidence for the static-review fix set (R01–R28 / V01–V03).
# Covers the failure/edge matrix, not only happy-path units.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

FILTER='r01_ r02_ r03_ r04_ r05_ r06_ r07_ r08_ r09_ r10_ r11_ r12_ r13_ r14_ r15_ r16_ r17_ r18_ r19_ r20_ r21_ r22_ r23_ r24_ r25_ r26_ r27_ r28_ v01_ v02_ v03_'

echo "== named rxx/vxx regressions =="
cargo test --workspace -- ${FILTER} -- --test-threads=8

echo "== kernel: stage error / panic / cancel / idle downstream fail =="
cargo test -p sparrow-runtime \
  r01_stage_error_is_job_failed_not_ok_cancelled \
  r01_stage_panic_is_job_failed \
  r01_user_stop_is_cancelled_ok \
  r01_downstream_fail_while_upstream_idle_shuts_down \
  -- --nocapture

echo "== resources: quantum, capture, memory, mailbox chunks, queue pressure =="
cargo test -p sparrow-runtime \
  r02_quantum_allows_more_events_than_old_lifetime_cap \
  r03_supervisor_style_capture_is_disabled \
  r04_small_reservation_rejects_unfinished_builder \
  r05_small_retention_rejects_minmax_growth \
  r17_many_keys_closing_chunk_within_mailbox \
  r25_blocked_sink_records_queue_pressure \
  -- --nocapture

echo "== SQL / eval: JOIN WHERE, window projection, int edges, abs =="
cargo test -p sparrow-sql r08_join_where_is_applied_not_dropped r09_window_select_keeps_alias_order r07_empty_abs_rejected_at_bind -- --nocapture
cargo test -p sparrow-expr r06_int_add_does_not_go_through_f64 r06_checked_int_overflow r06_fast_filter_eq_matches_evaluator_for_large_ints r07_empty_abs_and_coalesce_are_errors -- --nocapture

echo "== connectors: MQTT half-frame / silent CONNACK; HTTP push; redirect =="
cargo test -p sparrow-connectors \
  r19_stop_completes_when_broker_silent_after_accept \
  r19_half_frame_survives_ping_select \
  r19_coalesced_publishes_are_not_dropped \
  r20_full_queue_is_not_2xx \
  r21_method_and_path_enforced \
  r21_slow_conn_times_out \
  r21_over_concurrency_is_503 \
  r21_stop_drains_in_flight_tasks \
  v01_http_sink_does_not_follow_redirect_to_disallowed \
  -- --nocapture
cargo test -p sparrow-control v02_default_mqtt_client_id_is_unique_per_instance -- --nocapture

echo "== catalog: crash rollback, desired rev, slow SQLite =="
cargo test -p sparrow-control \
  r22_crash_before_commit_keeps_old_revision \
  r23_desired_revision_spec_is_not_latest \
  r26_slow_catalog_does_not_freeze_runtime \
  -- --nocapture

echo "== recovery protocol: flush-before-commit, abort vs CURRENT, PT timers =="
cargo test -p sparrow-runtime \
  r11_checkpoint_runs_flush_before_commit \
  r13_abort_after_current_keeps_committed \
  r16_pt_restore_rebuilds_timers \
  -- --nocapture

echo "== HTTP API recovery + metrics (not CLI-only) =="
cargo test -p sparrow-server --test review_api -- --nocapture

echo "== review-fix-demo: ok =="

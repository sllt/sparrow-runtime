#!/usr/bin/env bash
# Process / API evidence for the static-review fix set (R01–R28 / V01–V03).
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== unit/integration regressions (named rxx/vxx) =="
cargo test --workspace -- r01_ r02_ r03_ r04_ r05_ r06_ r07_ r08_ r09_ r10_ r11_ r12_ r13_ r14_ r15_ r16_ r17_ r18_ r19_ r20_ r21_ r22_ r23_ r24_ r25_ r26_ r27_ r28_ v01_ v02_ v03_ -- --test-threads=8

echo "== HTTP API: file+aligned create/start/checkpoint/kill/restore + desired revision + failed =="
cargo test -p sparrow-server --test review_api -- --nocapture

echo "== MQTT silent-broker stop deadline =="
cargo test -p sparrow-connectors r19_stop_completes_when_broker_silent_after_accept -- --nocapture

echo "== HTTP Push full queue is non-2xx =="
cargo test -p sparrow-connectors r20_full_queue_is_not_2xx -- --nocapture

echo "== JOIN WHERE applied; window projection =="
cargo test -p sparrow-sql r08_join_where_is_applied_not_dropped r09_window_select_keeps_alias_order -- --nocapture

echo "== kernel stage error is JobFailed; quantum does not kill =="
cargo test -p sparrow-runtime r01_stage_error_is_job_failed_not_ok_cancelled r02_quantum_allows_more_events_than_old_lifetime_cap r03_supervisor_style_capture_is_disabled -- --nocapture

echo "== review-fix-demo: ok =="

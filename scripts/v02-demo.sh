#!/usr/bin/env bash
# Real-process V0.2 demos. Asserts stdout, not only cargo test.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== V0.2 PT tumbling AVG (virtual clock) =="
out="$(cargo run -p sparrow-cli --bin v02_pt_tumble_avg --quiet)"
echo "$out"
echo "$out" | grep -q "FINAL a avg=15" || fail "missing FINAL a avg=15"
echo "$out" | grep -q "FINAL b avg=30" || fail "missing FINAL b avg=30"
echo "$out" | grep -q "recovery=none" || fail "missing recovery=none honesty"
echo "$out" | grep -q "v02_pt_tumble_avg: ok" || fail "pt tumble did not exit ok"

echo "== V0.2 count window =="
out="$(cargo run -p sparrow-cli --bin v02_count_window --quiet)"
echo "$out"
echo "$out" | grep -q "FINAL avgs=" || fail "missing count-window finals"
echo "$out" | grep -q "v02_count_window: ok" || fail "count window did not exit ok"

echo "== V0.2 bounded dedup (rejects unbounded) =="
out="$(cargo run -p sparrow-cli --bin v02_bounded_dedup --quiet)"
echo "$out"
echo "$out" | grep -q "rejected unbounded forever-dedup" || fail "did not reject unbounded dedup"
echo "$out" | grep -q "v02_bounded_dedup: ok" || fail "bounded dedup did not exit ok"

echo "== V0.2 static ReferenceTable =="
out="$(cargo run -p sparrow-cli --bin v02_static_table --quiet)"
echo "$out"
echo "$out" | grep -q "job1 (running keeps v1) site=west" || fail "job1 should keep v1 west"
echo "$out" | grep -q "job2 (new job uses v2) site=east" || fail "job2 should see v2 east"
echo "$out" | grep -q "v02_static_table: ok" || fail "static table did not exit ok"

echo "== V0.2 HTTP Push → kernel → MQTT Sink =="
out="$(cargo run -p sparrow-cli --bin v02_http_mqtt_loop --quiet)"
echo "$out"
echo "$out" | grep -q "mqtt subscriber saw" || fail "missing mqtt subscriber evidence"
echo "$out" | grep -q "v02_http_mqtt_loop: ok" || fail "http-mqtt loop did not exit ok"

echo "all V0.2 process demos passed"

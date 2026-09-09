#!/usr/bin/env bash
# Real-process V0.3 demos. Asserts stdout (injected event times, not wall clock).
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== V0.3 event-time tumble AVG (§51.2-style) =="
out="$(cargo run -p sparrow-cli --bin v03_et_tumble_avg --quiet)"
echo "$out"
echo "$out" | grep -q "FINAL d1 avg=80" || fail "missing FINAL d1 avg=80"
echo "$out" | grep -q "LATE t=8" || fail "missing LATE t=8 side output"
echo "$out" | grep -q "e+L" || fail "missing e+L step"
echo "$out" | grep -q "recovery=none" || fail "missing recovery=none honesty"
echo "$out" | grep -q "v03_et_tumble_avg: ok" || fail "et tumble did not exit ok"

echo "== V0.3 hopping overlap (planner bound) =="
out="$(cargo run -p sparrow-cli --bin v03_hop_overlap --quiet)"
echo "$out"
echo "$out" | grep -q "planner rejected hop overlap 9 > max 8" || fail "missing planner overlap reject"
echo "$out" | grep -q "overlap=2 accepted" || fail "missing accepted hop overlap"
echo "$out" | grep -q "FINAL hop \[0,10) avg=80" || fail "missing hop FINAL"
echo "$out" | grep -q "v03_hop_overlap: ok" || fail "hop demo did not exit ok"

echo "== V0.3 idle/active multi-input watermark =="
out="$(cargo run -p sparrow-cli --bin v03_idle_active --quiet)"
echo "$out"
echo "$out" | grep -q "effective=None (blocked)" || fail "missing uninitialized block"
echo "$out" | grep -q "effective=20s (idle excluded)" || fail "missing idle exclude"
echo "$out" | grep -q "all-idle" || fail "missing all-idle"
echo "$out" | grep -q "no WM going backward" || fail "missing monotonic evidence"
echo "$out" | grep -q "v03_idle_active: ok" || fail "idle/active did not exit ok"

echo "== V0.3 versioned lookup (as-of event time) =="
out="$(cargo run -p sparrow-cli --bin v03_versioned_lookup --quiet)"
echo "$out"
echo "$out" | grep -q "as-of t=1s site=west" || fail "missing v1 west"
echo "$out" | grep -q "as-of t=6s site=east" || fail "missing v2 east"
echo "$out" | grep -q "v03_versioned_lookup: ok" || fail "versioned lookup did not exit ok"

echo "all V0.3 process demos passed"

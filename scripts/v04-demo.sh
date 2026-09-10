#!/usr/bin/env bash
# Real-process V0.4 demos: experimental file checkpoint, MQTT reject, Graph explain.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

WORKDIR="${TMPDIR:-/tmp}/sparrow-v04-demo-$$"
mkdir -p "$WORKDIR"
export SPARROW_DATA_ROOTS="${SPARROW_DATA_ROOTS:-$WORKDIR}"
trap 'rm -rf "$WORKDIR"' EXIT

DATA="$WORKDIR/events.ndjson"
CHK="$WORKDIR/chk"
GRAPH="$WORKDIR/et.json"
mkdir -p "$CHK"

{
  echo '{"device_id":"d1","v":10}'
  echo '{"device_id":"d1","v":20}'
  echo '{"device_id":"d1","v":30}'
  echo '{"device_id":"d1","v":40}'
  echo '{"device_id":"d1","v":50}'
  echo '{"device_id":"d1","v":60}'
} > "$DATA"

echo "== V0.4 gold (no crash) =="
gold="$(cargo run -p sparrow-cli --bin v04_file_checkpoint --quiet -- --data "$DATA" --chk "$CHK/gold" --mode gold)"
echo "$gold"
echo "$gold" | grep -q "aligned" || fail "missing aligned label"
echo "$gold" | grep -q "exactly-once=rejected" || fail "missing exactly-once reject"
echo "$gold" | grep -q "FINALS count=2" || fail "gold should emit 2 count-window finals"
echo "$gold" | grep -q "v04_file_checkpoint: gold ok" || fail "gold did not exit ok"

echo "== V0.4 checkpoint then kill (process exit) =="
cut="$(cargo run -p sparrow-cli --bin v04_file_checkpoint --quiet -- --data "$DATA" --chk "$CHK/run" --mode checkpoint --until 2)"
echo "$cut"
echo "$cut" | grep -q "CHECKPOINT id=" || fail "missing checkpoint commit"
echo "$cut" | grep -q "ingested=2" || fail "checkpoint should stop after 2 records"
echo "$cut" | grep -q "CHECKPOINT state" || fail "missing checkpoint state fingerprint"
cut_state="$(echo "$cut" | grep "CHECKPOINT state" | tail -n1)"

echo "== V0.4 restore from committed checkpoint =="
rest="$(cargo run -p sparrow-cli --bin v04_file_checkpoint --quiet -- --data "$DATA" --chk "$CHK/run" --mode restore)"
echo "$rest"
echo "$rest" | grep -q "RESTORE position" || fail "missing restore position"
echo "$rest" | grep -q "ingested=2" || fail "restore should resume at ingested=2"
echo "$rest" | grep -q "FINALS count=2" || fail "restore should match gold finals count"
echo "$rest" | grep -q "v04_file_checkpoint: restore ok" || fail "restore did not exit ok"
rest_state="$(echo "$rest" | grep "RESTORE state" | tail -n1)"
# fingerprints share the same pos/ingested/keys prefix after restore
echo "$rest_state" | grep -q "ingested=2" || fail "restored state ingested mismatch"

echo "== V0.4 MQTT live_best_effort rejects restore =="
mqtt="$(cargo run -p sparrow-cli --bin v04_mqtt_reject --quiet)"
echo "$mqtt"
echo "$mqtt" | grep -q "REJECT mqtt_session" || fail "missing mqtt session reject"
echo "$mqtt" | grep -q "REJECT mqtt+aligned" || fail "missing mqtt aligned reject"
echo "$mqtt" | grep -q "cannot pretend durable restore" || fail "missing honesty line"
echo "$mqtt" | grep -q "v04_mqtt_reject: ok" || fail "mqtt reject did not exit ok"

echo "== V0.4 Graph validate/explain (V0.3 ET window) =="
cargo run -p sparrow-cli --bin v04_graph_author --quiet -- template > "$GRAPH"
val="$(cargo run -p sparrow-cli --bin v04_graph_author --quiet -- validate "$GRAPH")"
echo "$val"
echo "$val" | grep -q "accepted=true" || fail "graph validate rejected ET template"
exp="$(cargo run -p sparrow-cli --bin v04_graph_author --quiet -- explain "$GRAPH")"
echo "$exp"
echo "$exp" | grep -q "time=event-time" || fail "explain missing event-time"
echo "$exp" | grep -q "state=window_agg" || fail "explain missing window state"
echo "$exp" | grep -q "guarantee=live_best_effort" || fail "explain missing guarantee"
echo "$exp" | grep -q "physical=" || fail "explain missing physical"
echo "$exp" | grep -q "v04_graph_author: explain ok" || fail "graph explain did not exit ok"

echo "all V0.4 process demos passed"

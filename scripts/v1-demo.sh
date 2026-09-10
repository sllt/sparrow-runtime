#!/usr/bin/env bash
# Real-process V1 demos: production aligned file checkpoint, MQTT reject,
# soak/fault loops, and sparrow-server status/metrics/4xx.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $*" >&2; exit 1; }

WORKDIR="${TMPDIR:-/tmp}/sparrow-v1-demo-$$"
mkdir -p "$WORKDIR"
# N3: file/checkpoint paths require an explicit allowlist (cwd is never a default root).
export SPARROW_DATA_ROOTS="${SPARROW_DATA_ROOTS:-$WORKDIR}"
trap 'rm -rf "$WORKDIR"; kill "$SERVER_PID" 2>/dev/null || true' EXIT

DATA="$WORKDIR/events.ndjson"
ET="$WORKDIR/et.ndjson"
CHK="$WORKDIR/chk"
mkdir -p "$CHK"

{
  echo '{"device_id":"d1","v":10}'
  echo '{"device_id":"d1","v":20}'
  echo '{"device_id":"d1","v":30}'
  echo '{"device_id":"d1","v":40}'
  echo '{"device_id":"d1","v":50}'
  echo '{"device_id":"d1","v":60}'
} > "$DATA"

{
  echo '{"device_id":"d1","temperature":70.0,"ts":0}'
  echo '{"device_id":"d1","temperature":71.0,"ts":400000}'
  echo '{"device_id":"d1","temperature":72.0,"ts":800000}'
  echo '{"device_id":"d1","temperature":73.0,"ts":1200000}'
  echo '{"device_id":"d1","temperature":74.0,"ts":1600000}'
  echo '{"device_id":"d1","temperature":75.0,"ts":2000000}'
} > "$ET"

echo "== V1 gold count window (no crash) =="
gold="$(cargo run -p sparrow-cli --bin v1_file_checkpoint --quiet -- --data "$DATA" --chk "$CHK/gold" --mode gold --window count)"
echo "$gold"
echo "$gold" | grep -q "recovery=aligned" || fail "missing production aligned policy"
echo "$gold" | grep -q "exactly-once=rejected" || fail "missing exactly-once reject"
echo "$gold" | grep -q "FINALS count=2" || fail "gold should emit 2 count-window finals"
echo "$gold" | grep -q "sparrow_metrics" || fail "missing structured metrics"
echo "$gold" | grep -q "v1_file_checkpoint: gold ok" || fail "gold did not exit ok"

echo "== V1 checkpoint then kill (process exit) =="
cut="$(cargo run -p sparrow-cli --bin v1_file_checkpoint --quiet -- --data "$DATA" --chk "$CHK/run" --mode checkpoint --until 2 --window count)"
echo "$cut"
echo "$cut" | grep -q "CHECKPOINT id=" || fail "missing checkpoint commit"
echo "$cut" | grep -q "ingested=2" || fail "checkpoint should stop after 2 records"

echo "== V1 restore from committed checkpoint =="
rest="$(cargo run -p sparrow-cli --bin v1_file_checkpoint --quiet -- --data "$DATA" --chk "$CHK/run" --mode restore --window count)"
echo "$rest"
echo "$rest" | grep -q "RESTORE position" || fail "missing restore position"
echo "$rest" | grep -q "ingested=2" || fail "restore should resume at ingested=2"
echo "$rest" | grep -q "FINALS count=2" || fail "restore should match gold finals count"
echo "$rest" | grep -q "v1_file_checkpoint: restore ok" || fail "restore did not exit ok"

echo "== V1 ET window gold / checkpoint / restore =="
et_gold="$(cargo run -p sparrow-cli --bin v1_file_checkpoint --quiet -- --data "$ET" --chk "$CHK/et-gold" --mode gold --window et)"
echo "$et_gold"
echo "$et_gold" | grep -q "v1_file_checkpoint: gold ok" || fail "ET gold failed"
cargo run -p sparrow-cli --bin v1_file_checkpoint --quiet -- --data "$ET" --chk "$CHK/et-run" --mode checkpoint --until 3 --window et >/tmp/v1-et-cut.txt
et_rest="$(cargo run -p sparrow-cli --bin v1_file_checkpoint --quiet -- --data "$ET" --chk "$CHK/et-run" --mode restore --window et)"
echo "$et_rest"
echo "$et_rest" | grep -q "v1_file_checkpoint: restore ok" || fail "ET restore failed"
gold_n="$(echo "$et_gold" | grep '^FINALS count=' | tail -n1)"
rest_n="$(echo "$et_rest" | grep '^FINALS count=' | tail -n1)"
test "$gold_n" = "$rest_n" || fail "ET finals count mismatch: gold=$gold_n restore=$rest_n"

echo "== V1 MQTT live_best_effort rejects restore =="
mqtt="$(cargo run -p sparrow-cli --bin v1_mqtt_reject --quiet)"
echo "$mqtt"
echo "$mqtt" | grep -q "REJECT mqtt_session" || fail "missing mqtt session reject"
echo "$mqtt" | grep -q "REJECT mqtt+aligned" || fail "missing mqtt aligned reject"
echo "$mqtt" | grep -q "cannot pretend durable restore" || fail "missing honesty line"
echo "$mqtt" | grep -q "v1_mqtt_reject: ok" || fail "mqtt reject did not exit ok"

echo "== V1 soak / fault (finite) =="
soak="$(cargo run -p sparrow-cli --bin v1_soak --quiet)"
echo "$soak"
echo "$soak" | grep -q "v1_soak: ok" || fail "soak did not exit ok"
echo "$soak" | grep -q "disk-full" || fail "missing disk-full reject"
echo "$soak" | grep -q "corrupt-manifest" || fail "missing corrupt-manifest reject"

echo "== V1 sparrow-server status / metrics / 4xx =="
TOKEN="${SPARROW_TOKEN:-v1-demo-token}"
PORT="${SPARROW_PORT:-43181}"
BIND="127.0.0.1:${PORT}"
CATALOG="$WORKDIR/catalog.db"
LOG="$WORKDIR/server.log"
export SPARROW_TOKEN="$TOKEN"
cargo build -p sparrow-server
./target/debug/sparrow-server --bind "$BIND" --token "$TOKEN" --catalog "$CATALOG" --demo-io >"$LOG" 2>&1 &
SERVER_PID=$!
BASE="http://${BIND}"
AUTH=("Authorization: Bearer ${TOKEN}" "content-type: application/json")
for i in $(seq 1 80); do
  if curl -sf "$BASE/v1/health" >/dev/null; then
    break
  fi
  sleep 0.15
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$LOG" >&2
    fail "server exited"
  fi
done
curl -sf "$BASE/v1/health" | tee "$WORKDIR/health.json"
grep -q live_best_effort "$WORKDIR/health.json" || fail "health missing delivery"
grep -q restart_fresh "$WORKDIR/health.json" || fail "health missing default recovery"
grep -q aligned "$WORKDIR/health.json" || fail "health missing aligned"

CODE=$(curl -s -o "$WORKDIR/unauth.json" -w '%{http_code}' \
  -X GET "$BASE/v1/metrics")
test "$CODE" = "401" || fail "metrics without token should be 401"

curl -sf -H "${AUTH[0]}" "$BASE/v1/metrics" | tee "$WORKDIR/metrics.json"
grep -q jobs_started "$WORKDIR/metrics.json" || fail "metrics missing jobs_started"
grep -q sparrow_metrics "$WORKDIR/metrics.json" || fail "metrics missing structured log"

curl -sf -H "${AUTH[0]}" -H "${AUTH[1]}" -X PUT --data '{
  "fields": [
    {"name":"device_id","type":"utf8","nullable":false},
    {"name":"v","type":"int64","nullable":false}
  ]
}' "$BASE/v1/streams/sensors" >/dev/null

FILE_SPEC='{
  "version": 1,
  "stream": "sensors",
  "sql": "SELECT COUNT(*) AS n, device_id FROM sensors GROUP BY device_id, COUNT_WINDOW(2)",
  "source": { "kind": "file", "path": "'"$DATA"'" },
  "sink": { "kind": "log" },
  "delivery": "live_best_effort",
  "recovery": "aligned"
}'
curl -sf -H "${AUTH[0]}" -H "${AUTH[1]}" -X PUT --data "$FILE_SPEC" "$BASE/v1/pipelines/replay" >/dev/null
curl -sf -H "${AUTH[0]}" "$BASE/v1/pipelines/replay/status" | tee "$WORKDIR/status.json"
grep -q '"recovery":"aligned"' "$WORKDIR/status.json" || fail "status missing aligned recovery"
grep -q committed_checkpoint_only "$WORKDIR/status.json" || fail "status missing recovery_risk"
grep -q '"exactly_once":false' "$WORKDIR/status.json" || fail "status must deny exactly-once"

MQTT_ALIGN='{
  "version": 1,
  "stream": "sensors",
  "sql": "SELECT device_id FROM sensors",
  "source": { "kind": "mqtt", "use_demo_io": true, "topic": "sensors/json" },
  "sink": { "kind": "http", "use_demo_io": true },
  "delivery": "live_best_effort",
  "recovery": "aligned",
  "restore": { "kind": "checkpoint", "snapshot_id": "x" }
}'
CODE=$(curl -s -o "$WORKDIR/mqtt422.json" -w '%{http_code}' \
  -H "${AUTH[0]}" -H "${AUTH[1]}" -X POST --data "$MQTT_ALIGN" "$BASE/v1/validate")
test "$CODE" = "422" || fail "MQTT+aligned should be 422, got $CODE"
grep -q unsupported_restore "$WORKDIR/mqtt422.json" || fail "MQTT+aligned missing unsupported_restore"

XO='{
  "version": 1,
  "stream": "sensors",
  "sql": "SELECT device_id FROM sensors",
  "source": { "kind": "file", "path": "'"$DATA"'" },
  "sink": { "kind": "log" },
  "delivery": "exactly_once",
  "recovery": "aligned"
}'
CODE=$(curl -s -o "$WORKDIR/xo.json" -w '%{http_code}' \
  -H "${AUTH[0]}" -H "${AUTH[1]}" -X POST --data "$XO" "$BASE/v1/validate")
test "$CODE" = "422" || fail "exactly_once should be 422, got $CODE"

echo "all V1 process demos passed"

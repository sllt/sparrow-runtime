#!/usr/bin/env bash
# Boot sparrow-server and drive the V0.1 API happy path + key rejects.
set -euo pipefail
cd "$(dirname "$0")/.."

TOKEN="${SPARROW_TOKEN:-m3-dev-token}"
PORT="${SPARROW_PORT:-43180}"
BIND="127.0.0.1:${PORT}"
CATALOG="${SPARROW_CATALOG:-$(mktemp /tmp/sparrow-m3.XXXXXX.db)}"
LOG="${SPARROW_SERVER_LOG:-/tmp/sparrow-server-m3.log}"
export SPARROW_TOKEN="$TOKEN"

AUTH=("Authorization: Bearer ${TOKEN}" "content-type: application/json")
BASE="http://${BIND}"

echo "== build sparrow-server =="
cargo build -p sparrow-server

echo "== start sparrow-server --demo-io catalog=${CATALOG} =="
./target/debug/sparrow-server \
  --bind "$BIND" \
  --token "$TOKEN" \
  --catalog "$CATALOG" \
  --demo-io \
  >"$LOG" 2>&1 &
PID=$!
cleanup() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_health() {
  local i
  for i in $(seq 1 80); do
    if curl -sf "$BASE/v1/health" >/dev/null; then
      return 0
    fi
    sleep 0.15
  done
  echo "server failed to become healthy; log:" >&2
  cat "$LOG" >&2
  return 1
}

json() {
  curl -sf -H "${AUTH[0]}" -H "${AUTH[1]}" "$@"
}

wait_health
echo "-- health --"
curl -sf "$BASE/v1/health" | tee /tmp/m3-health.json
grep -q live_best_effort /tmp/m3-health.json
grep -q restart_fresh /tmp/m3-health.json

echo
echo "-- unauthorized mutate --"
CODE=$(curl -s -o /tmp/m3-unauth.json -w '%{http_code}' \
  -X PUT -H 'content-type: application/json' \
  --data '{"fields":[]}' "$BASE/v1/streams/nope")
test "$CODE" = "401"
grep -q policy_denied /tmp/m3-unauth.json
echo "    HTTP $CODE policy_denied"

echo
echo "-- create stream + pipeline --"
json -X PUT --data '{
  "fields": [
    {"name":"device_id","type":"utf8","nullable":false},
    {"name":"temperature","type":"float64","nullable":true},
    {"name":"humidity","type":"float64","nullable":true},
    {"name":"ts","type":"timestamp_micros_utc","nullable":false},
    {"name":"payload","type":"dynamic","nullable":true}
  ]
}' "$BASE/v1/streams/sensors" >/tmp/m3-stream.json

SPEC='{
  "version": 1,
  "stream": "sensors",
  "sql": "SELECT device_id, temperature, ts FROM sensors WHERE temperature > 25",
  "source": {"kind":"mqtt","use_demo_io":true,"topic":"sensors/json","client_id":"m3-demo"},
  "sink": {"kind":"http","use_demo_io":true},
  "delivery": "live_best_effort",
  "recovery": "restart_fresh"
}'
json -X PUT --data "$SPEC" "$BASE/v1/pipelines/hot" | tee /tmp/m3-pipe.json
grep -q rev-1 /tmp/m3-pipe.json

echo
echo "-- reject at-least-once + checkpoint restore --"
CODE=$(curl -s -o /tmp/m3-als.json -w '%{http_code}' \
  -H "${AUTH[0]}" -H "${AUTH[1]}" \
  --data '{"version":1,"stream":"sensors","sql":"SELECT device_id FROM sensors","source":{"kind":"mqtt","use_demo_io":true},"sink":{"kind":"http","use_demo_io":true},"delivery":"at_least_once","recovery":"restart_fresh"}' \
  "$BASE/v1/validate")
test "$CODE" = "422"
grep -q unsupported_delivery /tmp/m3-als.json
echo "    at_least_once -> $CODE unsupported_delivery"

CODE=$(curl -s -o /tmp/m3-ck.json -w '%{http_code}' \
  -H "${AUTH[0]}" -H "${AUTH[1]}" \
  --data '{"version":1,"stream":"sensors","sql":"SELECT device_id FROM sensors","source":{"kind":"mqtt","use_demo_io":true},"sink":{"kind":"http","use_demo_io":true},"delivery":"live_best_effort","recovery":"restart_fresh","restore":{"kind":"checkpoint","snapshot_id":"snap-1"}}' \
  "$BASE/v1/validate")
test "$CODE" = "422"
grep -q unsupported_restore /tmp/m3-ck.json
echo "    checkpoint restore -> $CODE unsupported_restore"

echo
echo "-- start (catalog commit, then supervisor converges) --"
json -X POST --data '{}' "$BASE/v1/pipelines/hot/start" | tee /tmp/m3-start.json
grep -q live_best_effort /tmp/m3-start.json

for i in $(seq 1 50); do
  STATUS=$(json "$BASE/v1/pipelines/hot/status")
  echo "$STATUS" > /tmp/m3-status.json
  if grep -q '"actual":{"revision":1,"status":"running"' /tmp/m3-status.json \
     || echo "$STATUS" | grep -q '"status":"running"'; then
    if echo "$STATUS" | grep -q '"actual"' && echo "$STATUS" | grep -q running; then
      break
    fi
  fi
  sleep 0.15
done
grep -q running /tmp/m3-status.json

echo
echo "-- publish fixture + wait HTTP capture --"
json -X POST --data '{}' "$BASE/v1/demo/publish-fixture" >/tmp/m3-pub.json
for i in $(seq 1 50); do
  json "$BASE/v1/demo/capture" > /tmp/m3-cap.json
  if grep -q edge-a /tmp/m3-cap.json && grep -q 26.2 /tmp/m3-cap.json \
     && grep -q edge-b /tmp/m3-cap.json && grep -q edge-c /tmp/m3-cap.json; then
    break
  fi
  sleep 0.15
done
grep -q edge-a /tmp/m3-cap.json
grep -q 26.2 /tmp/m3-cap.json
grep -q edge-b /tmp/m3-cap.json
grep -q edge-c /tmp/m3-cap.json
echo "    captured filtered sensor JSON"

echo
echo "-- stop --"
json -X POST --data '{}' "$BASE/v1/pipelines/hot/stop" >/tmp/m3-stop.json

echo
echo "-- restart server (same catalog): definitions reload, attempt is fresh --"
kill "$PID"
wait "$PID" 2>/dev/null || true
./target/debug/sparrow-server \
  --bind "$BIND" \
  --token "$TOKEN" \
  --catalog "$CATALOG" \
  --demo-io \
  >"$LOG" 2>&1 &
PID=$!
wait_health
json "$BASE/v1/pipelines/hot" | tee /tmp/m3-reloaded.json
grep -q '"name":"hot"' /tmp/m3-reloaded.json || grep -q hot /tmp/m3-reloaded.json
grep -q restart_fresh /tmp/m3-reloaded.json
# desired should still be stopped after explicit stop; start again = new attempt
json -X POST --data '{}' "$BASE/v1/pipelines/hot/start" >/tmp/m3-start2.json
for i in $(seq 1 50); do
  json "$BASE/v1/pipelines/hot/status" > /tmp/m3-status2.json
  if grep -q running /tmp/m3-status2.json; then
    break
  fi
  sleep 0.15
done
grep -q running /tmp/m3-status2.json
grep -q restart_fresh /tmp/m3-status2.json
echo "    reloaded definition; new attempt is restart_fresh (not restore)"

echo
echo "m3-demo: ok"
echo "binary=sparrow-server bind=${BIND} catalog=${CATALOG}"
echo "honesty: live_best_effort + restart_fresh; MQTT replay unsupported"

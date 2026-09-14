#!/usr/bin/env bash
# No builds. Use immutable, already-tested binaries; every workload gets a fresh
# directory. The driver owns its broker/server children, never existing services.
# Usage: OBS_NETEM=1 bash scripts/obs-closure-verify.sh ARTIFACT_DIR BEFORE_SERVER
set -euo pipefail
art=${1:?artifact directory containing driver and sparrow-server-production}
before=${2:?immutable baseline server binary}
phase=${OBS_PHASE:-all}
case "$phase" in all|io) ;; *) printf 'OBS_PHASE must be all or io\n' >&2; exit 2;; esac
driver="$art/driver"
after="$art/sparrow-server-production"
test -x "$driver" && test -x "$after" && test -x "$before"
test ! -e "$art/validation-plan.json"
printf '%s\n' '{"scope":"single-host-observation-regression-not-production-certification","file":{"order":["before","after","after","before"],"events":80000,"window":800,"rounds":2,"investigate_throughput_ratio_below":0.90,"investigate_rss_delta_kib_above":1024},"healthy_mqtt":{"events":10000,"rate":2000,"required_missing":0,"required_duplicate":0},"pressure":"intentional_best_effort_overload_not_lossless_pass","netem":"optional isolated network namespace, 10ms loopback egress delay; not WAN certification","continuous":{"events":120000,"rate":1000,"seconds":120},"new_global_host_qdisc":false}' > "$art/validation-plan.json"
printf '%s\n' "$phase" > "$art/requested-phase.txt"

run_driver() {
    local name=$1 server=$2
    shift 2
    "$driver" --engine sparrow --server-bin "$server" --out "$art/$name" "$@" > "$art/$name.log" 2>&1
}
exact() {
    jq -e -s 'length > 0 and all(.valid and .missing_rows == 0 and .duplicate_rows == 0 and .invalid_rows == 0)' "$art/$1/results.jsonl" > /dev/null
}

if [[ "$phase" == all ]]; then
  i=0
  for variant in before after after before; do
    i=$((i + 1))
    server="$after"; if [[ "$variant" == before ]]; then server="$before"; fi
    run_driver "file-$i-$variant" "$server" --scenarios file_count_window --file-events 80000 --window 800 --rounds 2 --metrics-ms 0 --observe-ms 0
    exact "file-$i-$variant"
    printf 'FILE_OK %s %s\n' "$i" "$variant"
  done
else
    printf 'FILE_STAGE_SKIPPED_BY_REQUEST (separate file evidence required)\n'
fi

run_driver healthy-mqtt "$after" --scenarios mqtt_filter_http --mqtt-events 10000 --rate 2000 --rounds 1 \
    --sink-delay-ms 20 --http-batch-rows 64 --http-linger-ms 10 --http-max-inflight 4 --quickack true --metrics-ms 100
exact healthy-mqtt
printf 'HEALTHY_MQTT_OK\n'

run_driver pressure "$after" --scenarios mqtt_http_queue_pressure --pressure-events 2048 --rounds 1 --drain-secs 4 --metrics-ms 100 --observe-ms 0 --quickack true
printf 'PRESSURE_RECORDED (inspect losses, do not call this lossless)\n'

if [[ "${OBS_NETEM:-0}" == 1 ]]; then
    test ! -e "$art/netem-mqtt"
    # All injected delay is confined to a fresh network namespace. Its loopback
    # and all listener ports are independent of the host/existing Mosquitto.
    if sudo -n unshare --net bash -c '
        set -euo pipefail
        art=$1; driver=$2; server=$3
        ip link set lo up
        if ! /usr/sbin/tc qdisc add dev lo root netem delay 10ms; then
            printf "NETEM_SETUP_UNAVAILABLE\n"; exit 77
        fi
        /usr/sbin/tc -s -j qdisc show dev lo > "$art/netem-before.json"
        "$driver" --engine sparrow --server-bin "$server" --out "$art/netem-mqtt" \
            --scenarios mqtt_filter_http --mqtt-events 1000 --rate 100 --rounds 1 --sink-delay-ms 0 \
            --http-batch-rows 64 --http-linger-ms 5 --http-max-inflight 4 --quickack true --metrics-ms 100
        /usr/sbin/tc -s -j qdisc show dev lo > "$art/netem-after.json"
    ' _ "$art" "$driver" "$after" > "$art/netem-mqtt.log" 2>&1; then
        exact netem-mqtt
        printf '%s\n' '{"status":"PASS","scope":"isolated_netem_not_WAN"}' > "$art/netem-status.json"
        printf 'ISOLATED_NETEM_OK\n'
    else
        result=$?
        if [[ "$result" != 77 ]]; then exit "$result"; fi
        printf '%s\n' '{"status":"NOT_RUN","reason":"netem_setup_unavailable","not_replaced_by_application_delay":true}' > "$art/netem-status.json"
        printf 'ISOLATED_NETEM_NOT_RUN (setup unavailable; not a passed latency test)\n'
    fi
fi

run_driver continuous-mqtt "$after" --scenarios mqtt_filter_http --mqtt-events 120000 --rate 1000 --rounds 1 \
    --sink-delay-ms 0 --http-batch-rows 64 --http-linger-ms 5 --http-max-inflight 4 --quickack true --metrics-ms 1000
exact continuous-mqtt
printf 'CONTINUOUS_120S_OK\n'

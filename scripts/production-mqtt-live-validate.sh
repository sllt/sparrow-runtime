#!/usr/bin/env bash
# No compilation. Exact frozen tests + a disposable pinned Mosquitto fixture,
# real server SIGKILL and 75s sustained traffic (NOT a 24/72h soak).
# Usage: ART PACKAGE FROZEN PROCESS_DRIVER [ROUNDS]
set -euo pipefail
[[ $# -eq 4 || $# -eq 5 ]]
root=$(cd "$(dirname "$0")/.." && pwd)
art=$1; package=$2; frozen=$3; driver=$4; rounds=${5:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
test -x "$driver"; test -x "$package/bin/sparrow-server"
mkdir -p "$art"; art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd)
container=""; image='eclipse-mosquitto@sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21'
cleanup() {
    code=$?
    if [[ -n "$container" ]]; then
        sudo -n docker logs "$container" > "$art/mosquitto.log" 2>&1 || true
        sudo -n docker inspect "$container" > "$art/container.json" 2>&1 || true
        sudo -n docker rm -f "$container" > "$art/container-cleanup.log" 2>&1 || true
    fi
    printf '%s\n' "$code" > "$art/exit"
}
trap cleanup EXIT
(cd "$package" && sha256sum -c SHA256SUMS) > "$art/package-verify.log"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
cp "$root/tests/mqtt-live/expected-tests.txt" "$art/expected-tests.txt"
LC_ALL=C sort "$art/expected-tests.txt" > "$art/expected.sorted"
test "$(wc -l < "$art/expected.sorted")" -eq 9
test -z "$(uniq -d "$art/expected.sorted")"
declare -A binaries
: > "$art/discovered"
while IFS=$'\t' read -r name exe; do
    case "$name" in
        sparrow_connectors) family='^mqtt::live::tests::' ;;
        sparrow_runtime) family='^kernel::live_silence::tests::' ;;
        sparrow_control) family='^paused_time_tests::live_silence_tests::' ;;
        *) continue ;;
    esac
    test -z "${binaries[$name]+present}"
    binary="$frozen/reliable-test-binaries/$(basename "$exe")"; test -x "$binary"; binaries[$name]=$binary
    "$binary" --list > "$art/list-$name"
    "$binary" --list --ignored > "$art/ignored-$name"
    if grep -Eq "${family}.*: test$" "$art/ignored-$name"; then exit 1; fi
    grep -E "${family}.*: test$" "$art/list-$name" | sed -e 's/: test$//' -e "s/^/$name|/" >> "$art/discovered"
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
LC_ALL=C sort "$art/discovered" > "$art/discovered.sorted"
diff -u "$art/expected.sorted" "$art/discovered.sorted" > "$art/inventory-diff.log"
mkdir "$art/tests"
for ((round=1; round<=rounds; round++)); do
    while IFS='|' read -r crate name; do
        log="$art/tests/$round-$crate-${name//:/_}.log"
        "${binaries[$crate]}" --exact "$name" --test-threads=1 > "$log" 2>&1
        grep -q '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$log"
    done < "$art/expected.sorted"
done
sudo -n docker image inspect "$image" > "$art/mosquitto-image.json"
printf 'listener 1883\nallow_anonymous true\npersistence false\n' > "$art/mosquitto.conf"
chmod 644 "$art/mosquitto.conf"
container="sparrow-mqtt-live-$(date +%s)-$$"
port=$(shuf -i 20000-60000 -n 1)
sudo -n docker run -d --name "$container" --restart=no --label sparrow.validation=mqtt-live \
    -p "127.0.0.1:$port:1883" --mount "type=bind,src=$art/mosquitto.conf,dst=/mosquitto/config/mosquitto.conf,readonly" \
    "$image" > "$art/container-id"
actual_port=$(sudo -n docker inspect --format '{{(index (index .NetworkSettings.Ports "1883/tcp") 0).HostPort}}' "$container")
test "$port" = "$actual_port"; sleep 1
sha256sum "$driver" "$package/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --mqtt-live-only --server-bin "$package/bin/sparrow-server" --mqtt-address "127.0.0.1:$port" \
    --mqtt-container "$container" --out "$art/process" > "$art/process.log" 2>&1
grep -qx MQTT_LIVE_PROCESS_OK "$art/process.log"
jq -e '.valid == true and .actual_sigkill == true and .crash_scenarios == 1 and .broker_outage_tested == true
    and .retained_ignored == true and .restart_new_generation == true and .restart_full_grace == true
    and .checkpoint_rejected == true and .sustained_seconds >= 75 and .sustained_sent > 1000
    and .sustained_reconnects == 0 and .false_silence == 0 and .durable_replay == false and .certified == false' \
    "$art/process/summary.json" > /dev/null
jq -n --argjson rounds "$rounds" '{valid:true,tests_per_round:9,rounds:$rounds,
    actual_sigkill:true,broker_outage_tested:true,sustained_seconds:75,performance:false,soak:false,certified:false}' > "$art/summary.json"
printf 'MQTT_LIVE_VALIDATION_OK\n'

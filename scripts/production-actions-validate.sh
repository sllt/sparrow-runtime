#!/usr/bin/env bash
# Frozen exact tests + independent production process/disk/wire oracle.
# ART DEFAULT_PACKAGE JS_PACKAGE FROZEN PROCESS_DRIVER [ROUNDS]
set -euo pipefail
[[ $# -eq 5 || $# -eq 6 ]]
root=$(cd "$(dirname "$0")/.." && pwd)
art=$1; default=$2; js=$3; frozen=$4; driver=$5; rounds=${6:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
for binary in "$driver" "$default/bin/sparrow-server" "$js/bin/sparrow-server"; do test -x "$binary"; done
mkdir -p "$art"; art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd)
container=""
cleanup() { code=$?; if [[ -n "$container" ]]; then sudo -n docker logs "$container" >"$art/broker.log" 2>&1 || true; sudo -n docker rm -f "$container" >"$art/broker-cleanup.log" 2>&1 || true; fi; printf '%s\n' "$code" >"$art/exit"; }
trap cleanup EXIT
for package in "$default" "$js"; do (cd "$package" && sha256sum -c SHA256SUMS) >> "$art/package-verify.log"; done
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
LC_ALL=C sort "$root/tests/actions/expected-tests.txt" > "$art/expected.sorted"
test "$(wc -l < "$art/expected.sorted")" -eq 27; test -z "$(uniq -d "$art/expected.sorted")"
declare -A binaries
: > "$art/discovered"
while IFS=$'\t' read -r name exe; do
    case "$name" in sparrow_connectors|sparrow_expr|sparrow_formats|sparrow_control|sparrow_runtime|sparrow_sql) ;; *) continue;; esac
    test -z "${binaries[$name]+present}"
    binary="$frozen/reliable-test-binaries/$(basename "$exe")"; test -x "$binary"; binaries[$name]=$binary
    "$binary" --list >"$art/list-$name"; "$binary" --list --ignored >"$art/ignored-$name"
    if grep -Eq '::actions_[^:]*: test$' "$art/ignored-$name"; then exit 1; fi
    grep -E '::actions_[^:]*: test$' "$art/list-$name" | sed -e 's/: test$//' -e "s/^/$name|/" >> "$art/discovered"
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
LC_ALL=C sort "$art/discovered" >"$art/discovered.sorted"
diff -u "$art/expected.sorted" "$art/discovered.sorted" >"$art/inventory-diff.log"
mkdir "$art/tests"
for ((round=1;round<=rounds;round++)); do
    while IFS='|' read -r crate name; do
        log="$art/tests/$round-$crate-${name//:/_}.log"
        "${binaries[$crate]}" --exact "$name" --test-threads=1 >"$log" 2>&1
        grep -q '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$log"
    done <"$art/expected.sorted"
done
image='eclipse-mosquitto@sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21'
sudo -n docker image inspect "$image" >"$art/mosquitto-image.json"
printf 'listener 1883\nallow_anonymous true\npersistence false\n' >"$art/mosquitto.conf"; chmod 644 "$art/mosquitto.conf"
container="sparrow-actions-$(date +%s)-$$"
sudo -n docker run -d --name "$container" --restart=no --label sparrow.validation=actions -p '127.0.0.1::1883' \
    --mount "type=bind,src=$art/mosquitto.conf,dst=/mosquitto/config/mosquitto.conf,readonly" "$image" >"$art/container-id"
port=$(sudo -n docker inspect --format '{{(index (index .NetworkSettings.Ports "1883/tcp") 0).HostPort}}' "$container"); sleep 1
sha256sum "$driver" "$default/bin/sparrow-server" "$js/bin/sparrow-server" >"$art/binaries.sha256"
for variant in default jetstream; do
    package=$default; if [[ "$variant" == jetstream ]]; then package=$js; fi
    "$driver" --actions-only --server-bin "$package/bin/sparrow-server" --mqtt-address "127.0.0.1:$port" --out "$art/$variant" >"$art/$variant.log" 2>&1
    grep -qx ACTIONS_PROCESS_OK "$art/$variant.log"
    jq -e '.valid == true and .scenarios == 3 and .actual_sigkill == true and .file_restart_no_overwrite == true
        and .partial_tail_rejected == true and .quota_failed == true and .http_query_retry_stable == true
        and .mqtt_real_broker == true and .aligned_rejected == true and .certified == false' "$art/$variant/summary.json" >/dev/null
done
jq -n --argjson rounds "$rounds" '{valid:true,tests_per_round:27,rounds:$rounds,process_scenarios:6,actual_sigkill:2,
    performance:false,soak:false,certified:false}' >"$art/summary.json"
printf 'ACTIONS_VALIDATION_OK\n'

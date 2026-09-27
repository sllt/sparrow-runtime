#!/usr/bin/env bash
# Frozen repeat gate only. No compilation and no process-crash/performance claim.
# Usage: bash scripts/production-paused-time-validate.sh ART FROZEN NATS [ROUNDS]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; frozen=${2:?frozen reliable tests}; nats=${3:?pinned NATS binary}; rounds=${4:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"; test -x "$nats"
command -v jq >/dev/null; command -v sha256sum >/dev/null
mkdir -p "$art"; art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
export SPARROW_NATS_SERVER="$nats"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
cp "$root/tests/paused-time/expected-tests.txt" "$art/expected-tests.txt"
sha256sum "$art/expected-tests.txt" "$nats" > "$art/inputs.sha256"
declare -A expected discovered executable ignored
count=0; regular=0; broker=0
while IFS='|' read -r crate name mode extra || [[ -n "$crate" ]]; do
    [[ -z "$crate" || "$crate" == \#* ]] && continue
    [[ -z "$extra" && "$name" == *paused_time_* && "$name" != *[[:space:]]* ]]
    case "$crate" in sparrow_plan|sparrow_runtime|sparrow_control) ;; *) exit 1;; esac
    case "$mode" in regular) regular=$((regular+1));; ignored) broker=$((broker+1));; *) exit 1;; esac
    key="$crate|$name"; [[ -z ${expected[$key]+x} ]]; expected[$key]=$mode; count=$((count+1))
done < "$art/expected-tests.txt"
test "$count" -gt 0; test "$broker" -gt 0
bins=(); i=0
paused_names() {
    # The new OFD1 tests share private File fixtures through this submodule,
    # but belong to the separate exact Silence inventory, not legacy TPD1.
    sed -n 's/^\([^[:space:]]*paused_time_[^[:space:]]*\): test$/\1/p' "$1" |
        sed -E '/^paused_time_tests::(observed_time_tests|resample_tests|live_silence_tests)::/d'
}
while IFS=$'\t' read -r target source; do
    crate=${target//-/_}; binary="$frozen/reliable-test-binaries/$(basename "$source")"; test -x "$binary"
    "$binary" --list paused_time_ > "$art/list-$i.txt"
    "$binary" --list --ignored paused_time_ > "$art/ignored-$i.txt"
    while IFS= read -r name; do ignored["$crate|$name"]=1; done < <(paused_names "$art/ignored-$i.txt")
    selected=0
    while IFS= read -r name; do
        key="$crate|$name"; [[ -z ${discovered[$key]+x} ]]; discovered[$key]=1; executable[$key]=$binary; selected=1
        [[ -n ${expected[$key]+x} ]] || { printf 'unexpected test: %s\n' "$key" >&2; exit 1; }
        if [[ ${expected[$key]} == ignored ]]; then [[ -n ${ignored[$key]+x} ]]; else [[ -z ${ignored[$key]+x} ]]; fi
    done < <(paused_names "$art/list-$i.txt")
    if [[ "$selected" == 1 ]]; then bins+=("$binary"); fi
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
[[ ${#discovered[@]} == "$count" && ${#ignored[@]} == "$broker" ]]
for key in "${!expected[@]}"; do [[ -n ${discovered[$key]+x} ]]; printf '%s|%s\n' "$key" "${expected[$key]}"; done | LC_ALL=C sort > "$art/verified-inventory.txt"
for ((round=1; round<=rounds; round++)); do
    for key in "${!expected[@]}"; do
        if [[ ${expected[$key]} != regular ]]; then continue; fi
        printf 'ROUND=%s TEST=%s\n' "$round" "$key" >> "$art/regular.log"
        "${executable[$key]}" "${key#*|}" --exact --test-threads=1 >> "$art/regular.log" 2>&1
    done
    for key in "${!ignored[@]}"; do
        printf 'ROUND=%s TEST=%s\n' "$round" "$key" >> "$art/broker.log"
        "${executable[$key]}" "${key#*|}" --ignored --exact --test-threads=1 >> "$art/broker.log" 2>&1
    done
done
jq -n --argjson rounds "$rounds" --argjson regular "$regular" --argjson broker "$broker" \
    '{valid:true,rounds:$rounds,regular_per_round:$regular,broker_per_round:$broker,
      scope:"unit_and_inprocess_connector_repeat",process_sigkill:false,performance:false,soak:false,certified:false}' > "$art/summary.json"
printf 'PAUSED_TIME_FROZEN_REPEAT_OK %s\n' "$art"

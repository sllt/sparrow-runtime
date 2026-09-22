#!/usr/bin/env bash
# Exact frozen inventory plus real process oracle, not production certification.
# Usage: ... ART PACKAGE_JS FROZEN DRIVER OLD_B2_SERVER NATS_SERVER [ROUNDS]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; package=${2:?JetStream package}; frozen=${3:?frozen tests}
driver=${4:?process oracle}; old=${5:?old B2 server}; nats=${6:?pinned NATS}; rounds=${7:-20}
expected=${SPARROW_COMPLETION_EXPECTED_TESTS:-$root/tests/k1-k4-reference-process/expected-tests.txt}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"; test -f "$expected"
for file in "$driver" "$old" "$nats" "$package/bin/sparrow-server"; do test -x "$file"; done
command -v jq >/dev/null; command -v sha256sum >/dev/null
mkdir -p "$art"; art=$(cd "$art" && pwd)
package=$(cd "$package" && pwd); frozen=$(cd "$frozen" && pwd)
(cd "$package" && sha256sum -c SHA256SUMS) > "$art/package-verify.log"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
cp "$expected" "$art/expected-tests.txt"; sha256sum "$expected" > "$art/expected-tests.sha256"
declare -A wanted found binary_for
count=0
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" == *\|* && "$line" != *\|*\|* ]]
    crate=${line%%|*}; name=${line#*|}
    [[ "$name" == *completion_* && "$name" != *[[:space:]]* ]]
    case "$crate" in sparrow_plan|sparrow_runtime|sparrow_control) ;; *) exit 1;; esac
    [[ -z ${wanted[$line]+x} ]] || { printf 'duplicate expected completion test %s\n' "$line" >&2; exit 1; }
    wanted[$line]=1; count=$((count+1))
done < "$expected"
test "$count" -gt 0
jq -e 'type=="array" and length>0 and all(.[]; (.name|type)=="string" and (.exe|type)=="string")' "$frozen/reliable-test-binaries.json" >/dev/null
bins=(); i=0
while IFS=$'\t' read -r target executable; do
    crate=${target//-/_}; [[ "$crate" =~ ^[a-zA-Z0-9_]+$ ]]
    binary="$frozen/reliable-test-binaries/$(basename "$executable")"; test -x "$binary"
    raw="$art/list-$i-$crate.txt"; ignored="$art/ignored-$i-$crate.txt"
    "$binary" --list completion_ > "$raw"
    "$binary" --list --ignored completion_ > "$ignored"
    if grep -q ': test$' "$ignored"; then printf 'ignored completion tests not admitted\n' >&2; exit 1; fi
    selected=0
    while IFS= read -r name; do
        key="$crate|$name"
        [[ -z ${found[$key]+x} ]] || { printf 'duplicate discovered completion test %s\n' "$key" >&2; exit 1; }
        found[$key]=1; selected=1
        [[ -z ${binary_for[$crate]+x} || ${binary_for[$crate]} == "$binary" ]] || exit 1
        binary_for[$crate]=$binary
    done < <(sed -n 's/^\([^[:space:]]*completion_[^[:space:]]*\): test$/\1/p' "$raw")
    if [[ "$selected" == 1 ]]; then bins+=("$binary"); fi
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
[[ ${#found[@]} == "$count" ]] || { printf 'completion inventory count mismatch\n' >&2; exit 1; }
for key in "${!wanted[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/expected.sorted.txt"
for key in "${!found[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/discovered.sorted.txt"
diff -u "$art/expected.sorted.txt" "$art/discovered.sorted.txt" > "$art/inventory-diff.log"
jq -n --argjson rounds "$rounds" --argjson count "$count" \
    '{profile:"reliable",rounds:$rounds,tests_per_round:$count,total:($rounds*$count),ignored:0,scope:"frozen_repeat_not_soak"}' > "$art/repeat-plan.json"
: > "$art/repeat.log"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" completion_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
sha256sum "$driver" "$old" "$nats" "$package/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --server-bin "$package/bin/sparrow-server" --old-server-bin "$old" \
    --nats-server "$nats" --out "$art/process" > "$art/process.log" 2>&1
jq -e 'type=="object" and .valid==true and .ttl_micros==0
    and .profile9_file_lookup_count_iot==true and .profile9_file_count_lookup==true
    and .profile10_jetstream_lookup_count==true and .profile10_jetstream_lookup_iot==true
    and .profile11_file_dag_branch==true and .profile11_file_dag_union==true
    and .profile11_file_dag_count_hysteresis==true
    and .profile12_hysteresis_file==true and .profile13_hysteresis_jetstream==true
    and ([.old_profile8_v9_guard,.old_profile8_v10_guard,.old_profile8_v11_guard,.new_profile8_v10_guard,.old_profile8_v12_guard,.old_profile8_v13_guard]
        |all(.checked==true and .history_preserved==true and .current_preserved==true and .output_preserved==true))
    and .snapshot_versions=={"profile9":9,"profile10":10,"profile11":11,"profile12":12,"profile13":13}
    and .exactly_once_claimed==false and .at_least_once_output_replay_explicit==true and .certified==false' \
    "$art/process/summary.json" >/dev/null
printf 'K1_K4_COMPLETION_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; bounded profiles, not release approval\n' "$rounds" "$count" "$((rounds*count))"

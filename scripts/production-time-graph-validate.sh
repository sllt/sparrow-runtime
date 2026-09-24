#!/usr/bin/env bash
# Frozen exact-inventory repeat and real-process SIGKILL; no compilation.
# Usage: ... ART PACKAGE FROZEN DRIVER OLD_SERVER NATS [ROUNDS]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; package=${2:?package}; frozen=${3:?frozen reliable tests}
driver=${4:?Go process driver}; old=${5:?v17 baseline}; nats=${6:?pinned broker}; rounds=${7:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
for item in "$driver" "$old" "$nats" "$package/bin/sparrow-server"; do test -x "$item"; done
mkdir -p "$art"; art=$(cd "$art" && pwd); package=$(cd "$package" && pwd); frozen=$(cd "$frozen" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
(cd "$package" && sha256sum -c SHA256SUMS) > "$art/package-verify.log"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
cp "$root/tests/time-graph/expected-tests.txt" "$art/expected-tests.txt"
LC_ALL=C sort "$art/expected-tests.txt" > "$art/expected.sorted.txt"
test "$(uniq -d "$art/expected.sorted.txt" | wc -l)" -eq 0
: > "$art/discovered.txt"
bins=(); i=0
while IFS=$'\t' read -r target executable; do
    crate=${target//-/_}; binary="$frozen/reliable-test-binaries/$(basename "$executable")"; test -x "$binary"
    "$binary" --list time_graph_ > "$art/list-$i.txt"
    "$binary" --list --ignored time_graph_ > "$art/ignored-$i.txt"
    if grep -q ': test$' "$art/ignored-$i.txt"; then printf 'ignored graph tests not admitted\n' >&2; exit 1; fi
    if grep -q ': test$' "$art/list-$i.txt"; then bins+=("$binary"); fi
    while IFS= read -r name; do printf '%s|%s\n' "$crate" "$name" >> "$art/discovered.txt"; done \
        < <(sed -n 's/^\([^[:space:]]*time_graph_[^[:space:]]*\): test$/\1/p' "$art/list-$i.txt")
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
LC_ALL=C sort "$art/discovered.txt" > "$art/discovered.sorted.txt"
diff -u "$art/expected.sorted.txt" "$art/discovered.sorted.txt" > "$art/inventory-diff.log"
count=$(wc -l < "$art/expected-tests.txt"); test "$count" -gt 0; test "${#bins[@]}" -gt 0
jq -n --argjson rounds "$rounds" --argjson count "$count" \
    '{rounds:$rounds,tests_per_round:$count,scope:"frozen_repeat_not_soak"}' > "$art/repeat-plan.json"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" time_graph_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
sha256sum "$driver" "$old" "$nats" "$package/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --time-graph-only --server-bin "$package/bin/sparrow-server" --old-server-bin "$old" \
    --nats-server "$nats" --out "$art/process" > "$art/process.log" 2>&1
jq -e '.valid and .process_scenarios==7 and .crash_scenarios==6 and .budget_refusal and .snapshot_versions==[18,19] and (.certified==false)
    and all(.old_profile_guards[];.checked and .history_preserved and .current_preserved and .output_preserved)' "$art/process/summary.json" >/dev/null
jq -n --argjson rounds "$rounds" --argjson count "$count" \
    '{valid:true,rounds:$rounds,tests_per_round:$count,process_scenarios:7,crash_scenarios:6,soak:false,certified:false}' > "$art/summary.json"
printf 'TIME_GRAPH_FROZEN_AND_PROCESS_OK\n'

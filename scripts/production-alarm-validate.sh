#!/usr/bin/env bash
# Exact frozen inventory + real process crashes. No compilation or soak claim.
# Usage: ART DEFAULT_PACKAGE JS_PACKAGE FROZEN DRIVER OLD_SERVER NATS [ROUNDS]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; default=${2:?default package}; js=${3:?JetStream package}
frozen=${4:?frozen tests}; driver=${5:?process driver}; old=${6:?old server}; nats=${7:?pinned broker}; rounds=${8:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
for item in "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server"; do test -x "$item"; done
mkdir -p "$art"; art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd)
default=$(cd "$default" && pwd); js=$(cd "$js" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
(cd "$default" && sha256sum -c SHA256SUMS) > "$art/default-verify.log"
(cd "$js" && sha256sum -c SHA256SUMS) > "$art/jetstream-verify.log"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
cp "$root/tests/alarm/expected-tests.txt" "$art/expected-tests.txt"
LC_ALL=C sort "$art/expected-tests.txt" > "$art/expected.sorted.txt"
test "$(uniq -d "$art/expected.sorted.txt" | wc -l)" -eq 0
: > "$art/discovered.txt"
bins=(); i=0
while IFS=$'\t' read -r target executable; do
    crate=${target//-/_}; binary="$frozen/reliable-test-binaries/$(basename "$executable")"; test -x "$binary"
    "$binary" --list alarm_ > "$art/list-$i.txt"
    "$binary" --list --ignored alarm_ > "$art/ignored-$i.txt"
    if grep -q ': test$' "$art/ignored-$i.txt"; then printf 'ignored alarm tests not admitted\n' >&2; exit 1; fi
    if grep -q ': test$' "$art/list-$i.txt"; then bins+=("$binary"); fi
    while IFS= read -r name; do printf '%s|%s\n' "$crate" "$name" >> "$art/discovered.txt"; done \
        < <(sed -n 's/^\([^[:space:]]*alarm_[^[:space:]]*\): test$/\1/p' "$art/list-$i.txt")
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
LC_ALL=C sort "$art/discovered.txt" > "$art/discovered.sorted.txt"
diff -u "$art/expected.sorted.txt" "$art/discovered.sorted.txt" > "$art/inventory-diff.log"
count=$(wc -l < "$art/expected-tests.txt"); test "$count" -gt 0; test "${#bins[@]}" -gt 0
jq -n --argjson rounds "$rounds" --argjson count "$count" '{rounds:$rounds,tests_per_round:$count,soak:false}' > "$art/repeat-plan.json"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" alarm_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
sha256sum "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --alarm-file-only --server-bin "$default/bin/sparrow-server" --old-server-bin "$old" \
    --nats-server "$nats" --out "$art/default-process" > "$art/default-process.log" 2>&1
"$driver" --alarm-only --server-bin "$js/bin/sparrow-server" --old-server-bin "$old" \
    --nats-server "$nats" --out "$art/jetstream-process" > "$art/jetstream-process.log" 2>&1
for variant in default jetstream; do
    expected=2; test "$variant" != jetstream || expected=4
    jq -e --argjson count "$expected" '.valid and .process_scenarios==$count and (.certified==false)
        and all(.old_profile_guards[];.checked and .history_preserved and .current_preserved and .output_preserved)' \
        "$art/$variant-process/summary.json" >/dev/null
done
jq -n --argjson rounds "$rounds" --argjson count "$count" \
    '{valid:true,rounds:$rounds,tests_per_round:$count,process_scenarios:6,graph_process_sigkill:false,soak:false,certified:false}' > "$art/summary.json"
printf 'ALARM_FROZEN_AND_PROCESS_OK\n'

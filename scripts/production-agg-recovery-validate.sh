#!/usr/bin/env bash
# Sub-batch 1 (extended aggregate recovery) validation on ONE frozen build.
# Repeats the frozen unit tests and every confirmable SIGKILL cut point
# ROUNDS times; failures are recorded, never re-run to be hidden.
# Usage:
#   SPARROW_NATS_SERVER=... SPARROW_AGG_TEST_BIN=frozen/sparrow_runtime-HASH \
#   bash scripts/production-agg-recovery-validate.sh NEW_ART SERVER_BIN [OLD_SERVER_BIN] [ROUNDS]
# SERVER_BIN must be built with --features jetstream; OLD_SERVER_BIN is a
# pre-v29 server (e.g. 424cf95) for rollback/upgrade checks.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); cd "$root"
art=${1:?new evidence directory}; server=${2:?jetstream-enabled sparrow-server}
old=${3:-}; rounds=${4:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
test -x "$server" || exit 2
test -x "${SPARROW_NATS_SERVER:-}" || { printf 'SPARROW_NATS_SERVER required\n' >&2; exit 4; }
test -x "${SPARROW_AGG_TEST_BIN:-}" || { printf 'SPARROW_AGG_TEST_BIN (frozen runtime test binary) required\n' >&2; exit 4; }
for tool in go sha256sum; do command -v "$tool" >/dev/null || exit 2; done
test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
mkdir -p "$art/frozen"
cp "$server" "$art/frozen/sparrow-server"; cp "$SPARROW_AGG_TEST_BIN" "$art/frozen/runtime-tests"
[[ -n "$old" ]] && cp "$old" "$art/frozen/sparrow-server-old"
server="$art/frozen/sparrow-server"; tests="$art/frozen/runtime-tests"
[[ -n "$old" ]] && old="$art/frozen/sparrow-server-old"
{ printf 'commit %s\n' "$(git rev-parse HEAD)"; git status --porcelain | sed 's/^/dirty /'; } > "$art/source.txt"
go build -o "$art/frozen/agg-recovery-process" tests/agg-recovery-process/main.go > "$art/go-build.log" 2>&1 || exit 3
go vet tests/agg-recovery-process/main.go > "$art/go-vet.log" 2>&1 || exit 3
(cd "$art/frozen" && sha256sum * ) > "$art/binaries.sha256"
sha256sum "$SPARROW_NATS_SERVER" >> "$art/binaries.sha256"
driver="$art/frozen/agg-recovery-process"
results="$art/results.tsv"; printf 'kind\tname\tround\texit\n' > "$results"
fail=0
# 1. Frozen unit tests (codec, compat, credit, restore equality).
"$tests" --list ext_agg_tests 2>/dev/null | grep -c ': test$' > "$art/unit-count.txt"
for ((r=1;r<=rounds;r++)); do
    "$tests" ext_agg_tests --test-threads=1 >> "$art/unit-repeat.log" 2>&1; code=$?
    printf 'unit\text_agg_tests\t%s\t%s\n' "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
done
# 2. Confirmable SIGKILL cut points.
cases=(file:count:input_after file:count:output_after file:count:commit_before file:count:commit_after
       file:et:input_after file:et:output_after file:et:commit_before file:et:commit_after
       jetstream:count:input_after jetstream:count:output_after jetstream:count:commit_before
       jetstream:count:commit_after jetstream:count:ack_lost)
for c in "${cases[@]}"; do
    IFS=: read -r source shape cut <<< "$c"
    for ((r=1;r<=rounds;r++)); do
        out="$art/process/$source-$shape-$cut/r$r"; mkdir -p "$(dirname "$out")"
        timeout 180 "$driver" --server-bin "$server" --nats-server "$SPARROW_NATS_SERVER" --source "$source" --shape "$shape" --cut "$cut" --out "$out" > "$out.log" 2>&1; code=$?
        printf 'process\t%s\t%s\t%s\n' "$c" "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
    done
done
# 3. Compatibility: upgrade/rollback with the old binary and profile/semantic mismatch.
timeout 300 "$driver" --server-bin "$server" ${old:+--old-server-bin "$old"} --cut compat --out "$art/compat" > "$art/compat.log" 2>&1; code=$?
printf 'compat\tupgrade_rollback\t1\t%s\n' "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
total=$(($(wc -l < "$results")-1))
printf 'AGG_RECOVERY_VALIDATE rounds=%s entries=%s failed=%s (SIGKILL, not power-loss)\n' "$rounds" "$total" "$fail" | tee "$art/summary.txt"
[[ $fail == 0 ]]

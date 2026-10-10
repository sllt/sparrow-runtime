#!/usr/bin/env bash
# Sub-batch 1 (extended aggregate recovery) validation on ONE frozen build.
# Repeats the frozen unit tests and every confirmable SIGKILL cut point
# ROUNDS times; failures are recorded, never re-run to be hidden.
# Usage:
#   SPARROW_NATS_SERVER=... SPARROW_AGG_TEST_BIN=frozen/sparrow_runtime-HASH \
#   bash scripts/production-agg-recovery-validate.sh NEW_ART SERVER_BIN OLD_SERVER_BIN [ROUNDS]
# SERVER_BIN must be built with --features jetstream,process-fault-pause (the
# pause feature is harness-only and never packaged); OLD_SERVER_BIN is a
# pre-v29 server (e.g. 4251ff1) for required rollback/upgrade checks. Optional
# CASES (space-separated source:shape:cut) overrides the case list.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); cd "$root"
art=${1:?new evidence directory}; server=${2:?jetstream-enabled sparrow-server}
old=${3:?pre-v29 sparrow-server required for upgrade/rollback}; rounds=${4:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
test -x "$server" || exit 2
test -x "$old" || exit 2
test -x "${SPARROW_NATS_SERVER:-}" || { printf 'SPARROW_NATS_SERVER required\n' >&2; exit 4; }
test -x "${SPARROW_AGG_TEST_BIN:-}" || { printf 'SPARROW_AGG_TEST_BIN (frozen runtime test binary) required\n' >&2; exit 4; }
for tool in go sha256sum timeout git awk grep sort sed tee wc; do command -v "$tool" >/dev/null || exit 2; done
# Exclusive creation: never reuse or overwrite evidence from a previous run.
mkdir -p "$(dirname "$art")"
mkdir "$art"; art=$(cd "$art" && pwd)
mkdir -p "$art/frozen"
cp "$server" "$art/frozen/sparrow-server"; cp "$SPARROW_AGG_TEST_BIN" "$art/frozen/runtime-tests"
cp "$old" "$art/frozen/sparrow-server-old"
cp "$SPARROW_NATS_SERVER" "$art/frozen/nats-server"
server="$art/frozen/sparrow-server"; tests="$art/frozen/runtime-tests"
old="$art/frozen/sparrow-server-old"
nats="$art/frozen/nats-server"
{ printf 'commit %s\n' "$(git rev-parse HEAD)"; git status --porcelain | sed 's/^/dirty /'; } > "$art/source.txt"
go build -o "$art/frozen/agg-recovery-process" tests/agg-recovery-process/main.go > "$art/go-build.log" 2>&1 || exit 3
go vet tests/agg-recovery-process/main.go tests/agg-recovery-process/main_test.go > "$art/go-vet.log" 2>&1 || exit 3
go test -count=1 tests/agg-recovery-process/main.go tests/agg-recovery-process/main_test.go > "$art/go-test.log" 2>&1 || exit 3
(cd "$art/frozen" && sha256sum * ) > "$art/binaries.sha256"
driver="$art/frozen/agg-recovery-process"
results="$art/results.tsv"; printf 'kind\tname\tround\texit\n' > "$results"
fail=0
# 1. Frozen unit tests (codec, compat, credit, restore equality).
timeout 30 "$tests" --list ext_agg_tests > "$art/unit-list.txt" 2> "$art/unit-list.err"
grep -c ': test$' "$art/unit-list.txt" > "$art/unit-count.txt" || {
    printf 'No ext_agg_tests matched: refusing an empty unit gate\n' >&2; exit 4;
}
for ((r=1;r<=rounds;r++)); do
    code=0
    timeout 180 "$tests" ext_agg_tests --test-threads=1 >> "$art/unit-repeat.log" 2>&1 || code=$?
    printf 'unit\text_agg_tests\t%s\t%s\n' "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
done
# 2. Confirmable SIGKILL cut points.
cuts="input_after output_after output_inflight commit_before manifest_renamed commit_after restore_kill"
cases=()
for c in $cuts; do cases+=("file:count:$c"); done
for c in $cuts; do cases+=("file:et:$c"); done
for c in $cuts; do cases+=("file:hop:$c"); done
for c in $cuts ack_lost; do cases+=("jetstream:count:$c"); done
[[ -n "${CASES:-}" ]] && read -r -a cases <<< "$CASES"
for c in "${cases[@]}"; do
    IFS=: read -r source shape cut <<< "$c"
    for ((r=1;r<=rounds;r++)); do
        out="$art/process/$source-$shape-$cut/r$r"; mkdir -p "$(dirname "$out")"
        code=0
        timeout 180 "$driver" --server-bin "$server" --nats-server "$nats" --source "$source" --shape "$shape" --cut "$cut" --out "$out" > "$out.log" 2>&1 || code=$?
        printf 'process\t%s\t%s\t%s\n' "$c" "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
    done
done
# 3. Compatibility: upgrade/rollback with the old binary and profile/semantic mismatch.
code=0
timeout 300 "$driver" --server-bin "$server" --old-server-bin "$old" --cut compat --out "$art/compat" > "$art/compat.log" 2>&1 || code=$?
printf 'compat\tupgrade_rollback\t1\t%s\n' "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
awk -F'\t' 'NR>1{k=$1"\t"$2; n[k]++; if($4==0)p[k]++; else f[k]=f[k]" r"$3"("$4")"} END{for(k in n) printf "%s\t%d\t%d\t%d\t%s\n",k,n[k],p[k]+0,n[k]-p[k],f[k]}' "$results" \
    | sort | { printf 'kind\tcase\trounds\tpass\tfail\tfailed_rounds(exit)\n'; cat; } > "$art/summary-table.tsv"
total=$(($(wc -l < "$results")-1))
printf 'AGG_RECOVERY_VALIDATE rounds=%s entries=%s failed=%s (SIGKILL, not power-loss)\n' "$rounds" "$total" "$fail" | tee "$art/summary.txt"
[[ $fail == 0 ]]

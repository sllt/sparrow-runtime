#!/usr/bin/env bash
# Sub-batch 2a (sliding count recovery, v31/v32) validation on ONE frozen
# build. Repeats the frozen sliding_count unit tests and every confirmable
# SIGKILL cut ROUNDS times; failures are recorded, never re-run to be hidden.
# Usage:
#   SPARROW_NATS_SERVER=... SPARROW_AGG_TEST_BIN=frozen/sparrow_runtime-HASH \
#   bash scripts/production-sliding-count-validate.sh NEW_ART SERVER_BIN OLD_SERVER_BIN PREV_SERVER_BIN [ROUNDS]
# SERVER_BIN: --features jetstream,process-fault-pause (harness-only feature).
# OLD_SERVER_BIN: pre-v29 (424cf95). PREV_SERVER_BIN: #29 (v29/v30, pre-v31).
# Optional CASES (space-separated source:cut) overrides the case list.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); cd "$root"
art=${1:?new evidence directory}; server=${2:?server}; old=${3:?old server}; prev=${4:?#29 server}
rounds=${5:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
for b in "$server" "$old" "$prev"; do test -x "$b" || exit 2; done
test -x "${SPARROW_NATS_SERVER:-}" || { printf 'SPARROW_NATS_SERVER required\n' >&2; exit 4; }
test -x "${SPARROW_AGG_TEST_BIN:-}" || { printf 'SPARROW_AGG_TEST_BIN required\n' >&2; exit 4; }
for tool in go sha256sum; do command -v "$tool" >/dev/null || exit 2; done
test ! -e "$art"; mkdir -p "$art/frozen"; art=$(cd "$art" && pwd)
cp "$server" "$art/frozen/sparrow-server"; cp "$SPARROW_AGG_TEST_BIN" "$art/frozen/runtime-tests"
cp "$old" "$art/frozen/sparrow-server-old-424cf95"; cp "$prev" "$art/frozen/sparrow-server-prev-29"
server="$art/frozen/sparrow-server"; tests="$art/frozen/runtime-tests"
old="$art/frozen/sparrow-server-old-424cf95"; prev="$art/frozen/sparrow-server-prev-29"
{ printf 'commit %s\n' "$(git rev-parse HEAD)"; git status --porcelain | sed 's/^/dirty /'; } > "$art/source.txt"
go build -o "$art/frozen/agg-recovery-process" tests/agg-recovery-process/main.go > "$art/go-build.log" 2>&1 || exit 3
go vet tests/agg-recovery-process/main.go tests/agg-recovery-process/main_test.go > "$art/go-vet.log" 2>&1 || exit 3
go test -count=1 tests/agg-recovery-process/main.go tests/agg-recovery-process/main_test.go > "$art/go-test.log" 2>&1 || exit 3
(cd "$art/frozen" && sha256sum * ) > "$art/binaries.sha256"
sha256sum "$SPARROW_NATS_SERVER" >> "$art/binaries.sha256"
driver="$art/frozen/agg-recovery-process"
results="$art/results.tsv"; printf 'kind\tname\tround\texit\n' > "$results"
fail=0
"$tests" --list sliding_count_tests 2>/dev/null | grep -c ': test$' > "$art/unit-count.txt"
for ((r=1;r<=rounds;r++)); do
    "$tests" sliding_count_tests --test-threads=1 >> "$art/unit-repeat.log" 2>&1; code=$?
    printf 'unit\tsliding_count_tests\t%s\t%s\n' "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
done
cuts="input_after output_after output_inflight commit_before manifest_renamed commit_after restore_kill empty not_full at_boundary low_budget"
cases=()
for c in $cuts; do cases+=("file:$c"); done
for c in $cuts ack_lost; do cases+=("jetstream:$c"); done
[[ -n "${CASES:-}" ]] && read -r -a cases <<< "$CASES"
for c in "${cases[@]}"; do
    IFS=: read -r source cut <<< "$c"
    for ((r=1;r<=rounds;r++)); do
        out="$art/process/$source-slide-$cut/r$r"; mkdir -p "$(dirname "$out")"
        timeout 180 "$driver" --server-bin "$server" --nats-server "$SPARROW_NATS_SERVER" --source "$source" --shape slide --cut "$cut" --out "$out" > "$out.log" 2>&1; code=$?
        printf 'process\t%s\t%s\t%s\n' "$source:slide:$cut" "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
    done
done
timeout 300 "$driver" --server-bin "$server" --old-server-bin "$old" --prev-server-bin "$prev" --cut compat_slide --out "$art/compat" > "$art/compat.log" 2>&1; code=$?
printf 'compat\tv31_old_prev_params\t1\t%s\n' "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
awk -F'\t' 'NR>1{k=$1"\t"$2; n[k]++; if($4==0)p[k]++; else f[k]=f[k]" r"$3"("$4")"} END{for(k in n) printf "%s\t%d\t%d\t%d\t%s\n",k,n[k],p[k]+0,n[k]-p[k],f[k]}' "$results" \
    | sort | { printf 'kind\tcase\trounds\tpass\tfail\tfailed_rounds(exit)\n'; cat; } > "$art/summary-table.tsv"
total=$(($(wc -l < "$results")-1))
printf 'SLIDING_COUNT_VALIDATE rounds=%s entries=%s failed=%s (SIGKILL, not power-loss)\n' "$rounds" "$total" "$fail" | tee "$art/summary.txt"
[[ $fail == 0 ]]

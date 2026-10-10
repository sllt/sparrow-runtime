#!/usr/bin/env bash
# Sub-batch 2b (ET sliding / ET session recovery, File v33) validation on ONE
# frozen build. Repeats the frozen buffered_et unit tests and every
# confirmable SIGKILL cut ROUNDS times per shape; failures are recorded,
# never re-run to be hidden.
# Usage:
#   SPARROW_AGG_TEST_BIN=frozen/sparrow_runtime-HASH \
#   bash scripts/production-et-buffered-validate.sh NEW_ART SERVER_BIN OLD_BIN MAIN_BIN PR30_BIN [ROUNDS]
# SERVER_BIN: --features jetstream,process-fault-pause (harness-only feature).
# OLD_BIN: pre-v29 (424cf95). MAIN_BIN: #29-merged main (v29/v30). PR30_BIN: #30 (v31/v32).
# Optional CASES (space-separated shape:cut) overrides the case list.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); cd "$root"
art=${1:?new evidence directory}; server=${2:?server}; old=${3:?old}; main=${4:?main}; prev=${5:?#30}
rounds=${6:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
for b in "$server" "$old" "$main" "$prev"; do test -x "$b" || exit 2; done
test -x "${SPARROW_AGG_TEST_BIN:-}" || { printf 'SPARROW_AGG_TEST_BIN required\n' >&2; exit 4; }
for tool in go sha256sum; do command -v "$tool" >/dev/null || exit 2; done
test ! -e "$art"; mkdir -p "$art/frozen"; art=$(cd "$art" && pwd)
cp "$server" "$art/frozen/sparrow-server"; cp "$SPARROW_AGG_TEST_BIN" "$art/frozen/runtime-tests"
cp "$old" "$art/frozen/sparrow-server-old-424cf95"; cp "$main" "$art/frozen/sparrow-server-main-29"; cp "$prev" "$art/frozen/sparrow-server-pr30"
server="$art/frozen/sparrow-server"; tests="$art/frozen/runtime-tests"
old="$art/frozen/sparrow-server-old-424cf95"; main="$art/frozen/sparrow-server-main-29"; prev="$art/frozen/sparrow-server-pr30"
{ printf 'commit %s\n' "$(git rev-parse HEAD)"; git status --porcelain | sed 's/^/dirty /'; } > "$art/source.txt"
go build -o "$art/frozen/agg-recovery-process" tests/agg-recovery-process/main.go > "$art/go-build.log" 2>&1 || exit 3
go vet tests/agg-recovery-process/main.go tests/agg-recovery-process/main_test.go > "$art/go-vet.log" 2>&1 || exit 3
go test -count=1 tests/agg-recovery-process/main.go tests/agg-recovery-process/main_test.go > "$art/go-test.log" 2>&1 || exit 3
(cd "$art/frozen" && sha256sum * ) > "$art/binaries.sha256"
driver="$art/frozen/agg-recovery-process"
results="$art/results.tsv"; printf 'kind\tname\tround\texit\n' > "$results"
fail=0
"$tests" --list buffered_et_tests 2>/dev/null | grep -c ': test$' > "$art/unit-count.txt"
for ((r=1;r<=rounds;r++)); do
    "$tests" buffered_et_tests --test-threads=1 >> "$art/unit-repeat.log" 2>&1; code=$?
    printf 'unit\tbuffered_et_tests\t%s\t%s\n' "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
done
common="input_after output_after output_inflight commit_before manifest_renamed commit_after restore_kill empty about_to_close no_new_input low_budget"
cases=()
for c in $common ooo_merged; do cases+=("sess:$c"); done
for c in $common; do cases+=("etslide:$c"); done
[[ -n "${CASES:-}" ]] && read -r -a cases <<< "$CASES"
for c in "${cases[@]}"; do
    IFS=: read -r shape cut <<< "$c"
    for ((r=1;r<=rounds;r++)); do
        out="$art/process/file-$shape-$cut/r$r"; mkdir -p "$(dirname "$out")"
        timeout 180 "$driver" --server-bin "$server" --source file --shape "$shape" --cut "$cut" --out "$out" > "$out.log" 2>&1; code=$?
        printf 'process\t%s\t%s\t%s\n' "file:$shape:$cut" "$r" "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
    done
done
timeout 400 "$driver" --server-bin "$server" --old-server-bin "$old" --main-server-bin "$main" --prev-server-bin "$prev" --cut compat_et --out "$art/compat" > "$art/compat.log" 2>&1; code=$?
printf 'compat\tv33_old_main_pr30_params\t1\t%s\n' "$code" >> "$results"; [[ $code == 0 ]] || fail=$((fail+1))
awk -F'\t' 'NR>1{k=$1"\t"$2; n[k]++; if($4==0)p[k]++; else f[k]=f[k]" r"$3"("$4")"} END{for(k in n) printf "%s\t%d\t%d\t%d\t%s\n",k,n[k],p[k]+0,n[k]-p[k],f[k]}' "$results" \
    | sort | { printf 'kind\tcase\trounds\tpass\tfail\tfailed_rounds(exit)\n'; cat; } > "$art/summary-table.tsv"
total=$(($(wc -l < "$results")-1))
printf 'ET_BUFFERED_VALIDATE rounds=%s entries=%s failed=%s (SIGKILL, not power-loss)\n' "$rounds" "$total" "$fail" | tee "$art/summary.txt"
[[ $fail == 0 ]]

#!/usr/bin/env bash
# Sub-batch 2c (PT hopping / sliding / session / tumbling+new aggs; File v34,
# JetStream v35, durable logical clock) validation on ONE frozen build.
# Repeats the frozen PT unit tests and the v14-v18 regression tests ROUNDS
# times, then every confirmable SIGKILL cut ROUNDS times; failures are
# recorded, never re-run to be hidden.
# Usage:
#   SPARROW_RUNTIME_TEST_BIN=... SPARROW_CONTROL_TEST_BIN=... \
#   bash scripts/production-pt-window-validate.sh NEW_ART SERVER_BIN NATS OLD_BIN MAIN_BIN PR30_BIN PR31_BIN [ROUNDS]
# SERVER_BIN: --features jetstream,process-fault-pause (harness-only feature).
# OLD: 424cf95. MAIN: #29-merged main. PR30: #30 (v31/v32). PR31: #31 (v33).
# Optional CASES (space-separated source:shape:cut) overrides the case list.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); cd "$root"
art=${1:?new evidence directory}; server=${2:?server}; nats=${3:?nats}; old=${4:?old}; main=${5:?main}; pr30=${6:?#30}; pr31=${7:?#31}
rounds=${8:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
for b in "$server" "$nats" "$old" "$main" "$pr30" "$pr31" "${SPARROW_RUNTIME_TEST_BIN:-}" "${SPARROW_CONTROL_TEST_BIN:-}"; do test -x "$b" || exit 2; done
for tool in go sha256sum; do command -v "$tool" >/dev/null || exit 2; done
test ! -e "$art"; mkdir -p "$art/frozen"; art=$(cd "$art" && pwd)
f="$art/frozen"
cp "$server" "$f/sparrow-server"; cp "$nats" "$f/nats-server"; cp "$old" "$f/sparrow-server-old-424cf95"; cp "$main" "$f/sparrow-server-main-29"
cp "$pr30" "$f/sparrow-server-pr30"; cp "$pr31" "$f/sparrow-server-pr31"
cp "$SPARROW_RUNTIME_TEST_BIN" "$f/runtime-tests"; cp "$SPARROW_CONTROL_TEST_BIN" "$f/control-tests"
{ printf 'commit %s\n' "$(git rev-parse HEAD)"; git status --porcelain | sed 's/^/dirty /'; } > "$art/source.txt"
go build -o "$f/pt-window-process" tests/pt-window-process/*.go > "$art/go-build.log" 2>&1 || exit 3
go vet tests/pt-window-process/*.go > "$art/go-vet.log" 2>&1 || exit 3
go test -count=1 tests/pt-window-process/*.go > "$art/go-test.log" 2>&1 || exit 3
(cd "$f" && sha256sum * ) > "$art/binaries.sha256"
driver="$f/pt-window-process"
results="$art/results.tsv"; printf 'kind\tname\tround\texit\n' > "$results"
fail=0
record() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$results"; [[ $4 == 0 ]] || fail=$((fail+1)); }
"$f/runtime-tests" --list pt_window_tests 2>/dev/null | grep -c ': test$' > "$art/unit-count.txt"
for ((r=1;r<=rounds;r++)); do
    "$f/runtime-tests" pt_window_tests --test-threads=2 >> "$art/unit-pt.log" 2>&1; record unit runtime:pt_window_tests "$r" $?
    # v14-v17 paused time (TPD1/PTC1) + v34 supervisor; v18 time graph.
    "$f/control-tests" paused_time_tests --test-threads=1 >> "$art/regress-paused.log" 2>&1; record regress control:paused_time_tests "$r" $?
    "$f/control-tests" time_graph_tests --test-threads=1 >> "$art/regress-graph.log" 2>&1; record regress control:time_graph_tests "$r" $?
    "$f/runtime-tests" paused_time --test-threads=2 >> "$art/regress-runtime.log" 2>&1; record regress runtime:paused_time "$r" $?
done
common="input_after output_after output_inflight manifest_renamed restore_kill fire_tick about_to_fire no_new_input empty low_budget"
cases=()
for s in pthop pttumble ptslide ptsess; do for c in $common; do cases+=("file:$s:$c"); done; done
cases+=("file:pthop:neg_start")
for s in pthop pttumble ptslide ptsess; do for c in input_after output_after fire_tick no_new_input ack_lost; do cases+=("jetstream:$s:$c"); done; done
[[ -n "${CASES:-}" ]] && read -r -a cases <<< "$CASES"
for c in "${cases[@]}"; do
    IFS=: read -r source shape cut <<< "$c"
    for ((r=1;r<=rounds;r++)); do
        out="$art/process/$source-$shape-$cut/r$r"; mkdir -p "$(dirname "$out")"
        timeout 240 "$driver" --server-bin "$f/sparrow-server" --nats-server "$f/nats-server" --source "$source" --shape "$shape" --cut "$cut" --out "$out" > "$out.log" 2>&1
        record process "$c" "$r" $?
    done
done
timeout 900 "$driver" --server-bin "$f/sparrow-server" --old-server-bin "$f/sparrow-server-old-424cf95" --main-server-bin "$f/sparrow-server-main-29" \
    --pr30-server-bin "$f/sparrow-server-pr30" --pr31-server-bin "$f/sparrow-server-pr31" --cut compat --out "$art/compat" > "$art/compat.log" 2>&1
record compat v34_old_main_pr30_pr31_params_v16 1 $?
awk -F'\t' 'NR>1{k=$1"\t"$2; n[k]++; if($4==0)p[k]++; else f[k]=f[k]" r"$3"("$4")"} END{for(k in n) printf "%s\t%d\t%d\t%d\t%s\n",k,n[k],p[k]+0,n[k]-p[k],f[k]}' "$results" \
    | sort | { printf 'kind\tcase\trounds\tpass\tfail\tfailed_rounds(exit)\n'; cat; } > "$art/summary-table.tsv"
total=$(($(wc -l < "$results")-1))
printf 'PT_WINDOW_VALIDATE rounds=%s entries=%s failed=%s (SIGKILL, not power loss)\n' "$rounds" "$total" "$fail" | tee "$art/summary.txt"
[[ $fail == 0 ]]

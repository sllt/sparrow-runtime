#!/usr/bin/env bash
# Real-broker tests are mandatory. Freeze once, repeat without Cargo, then use
# the identical production binary for process faults and capacity/latency.
# Usage: SPARROW_NATS_SERVER=... bash scripts/production-k2-validate.sh NEW_ART PACKAGE [ROUNDS]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd); cd "$root"
art=${1:?new evidence directory}; package=${2:?JetStream production package}; rounds=${3:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
if [[ ! -x "${SPARROW_NATS_SERVER:-}" ]]; then printf 'SKIPPED K2: pinned SPARROW_NATS_SERVER required\n' >&2; exit 4; fi
[[ $("$SPARROW_NATS_SERVER" --version) == *v2.14.6* ]] || { printf 'NATS 2.14.6 required\n' >&2; exit 2; }
for tool in cargo jq sha256sum go; do command -v "$tool" >/dev/null; done
test -x "$package/bin/sparrow-server"; (cd "$package" && sha256sum -c SHA256SUMS) >/dev/null
test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
package=$(cd "$package" && pwd)
export SPARROW_TEST_ARTIFACTS="$art/fixtures"
export SPARROW_DATA_ROOTS="$art/fixtures:${TMPDIR:-/tmp}/sparrow${SPARROW_DATA_ROOTS:+:$SPARROW_DATA_ROOTS}"
mkdir -p "$SPARROW_TEST_ARTIFACTS"
frozen=${SPARROW_K2_FROZEN_DIR:-$art/frozen}
if [[ -z ${SPARROW_K2_FROZEN_DIR:-} ]]; then
    bash scripts/production-freeze-tests.sh "$frozen" reliable > "$art/freeze.log" 2>&1
fi
frozen=$(cd "$frozen" && pwd)
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
bins=(); selected=0
while IFS= read -r file_path; do
    bin="$frozen/reliable-test-binaries/$(basename "$file_path")"
    test -x "$bin"
    "$bin" --list k2_ > "$art/$(basename "$bin").selected.txt"
    count=$(grep -c ': test$' "$art/$(basename "$bin").selected.txt" || true)
    if [[ "$count" -gt 0 ]]; then bins+=("$bin"); selected=$((selected+count)); fi
done < <(jq -r '.[].exe' "$frozen/reliable-test-binaries.json")
[[ "$selected" -ge 23 ]] || { printf 'Incomplete K2 test inventory\n' >&2; exit 2; }
jq -n --argjson rounds "$rounds" --argjson count "$selected" '{rounds:$rounds,tests_per_round:$count,include_ignored:true,filter:"k2_",scope:"deterministic_repeat_not_soak"}' > "$art/repeat-plan.json"
for ((round=1;round<=rounds;round++)); do
    for bin in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$bin" >> "$art/repeat.log"
        "$bin" k2_ --include-ignored --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
go build -o "$art/k2-process" tests/k2-process/main.go > "$art/go-build.log" 2>&1
go vet tests/k2-process/main.go > "$art/go-vet.log" 2>&1
sha256sum "$art/k2-process" "$package/bin/sparrow-server" "$SPARROW_NATS_SERVER" > "$art/binaries.sha256"
"$art/k2-process" --server-bin "$package/bin/sparrow-server" --nats-server "$SPARROW_NATS_SERVER" --out "$art/process" > "$art/process.log" 2>&1
rows=${SPARROW_K2_BENCH_ROWS:-8190}
for shape in zero count; do
    for rate in 0 2000 10000; do
        "$art/k2-process" --server-bin "$package/bin/sparrow-server" --nats-server "$SPARROW_NATS_SERVER" \
            --out "$art/bench-$shape-$rate" --bench-rows "$rows" --bench-shape "$shape" --rate "$rate" > "$art/bench-$shape-$rate.log" 2>&1
    done
done
printf 'K2_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; process and six loopback benchmarks passed (not TLS/WAN/soak)\n' "$rounds" "$selected" "$((rounds*selected))"

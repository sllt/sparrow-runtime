#!/usr/bin/env bash
# Repeat already-built, immutable observation regressions; never calls Cargo.
set -euo pipefail
art=${1:?artifact directory with frozen test binaries and manifests}
rounds=${2:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || { printf 'rounds must be 1..99\n' >&2; exit 2; }
test ! -e "$art/repeat.log"
bins=()
while IFS= read -r path; do bins+=("$art/core-test-binaries/$(basename "$path")"); done < <(
    jq -r '.[] | select(.name == "sparrow_model" or .name == "sparrow_io" or .name == "sparrow_plan"
        or .name == "sparrow_runtime" or .name == "sparrow_connectors" or .name == "sparrow_control"
        or .name == "review_api") | .exe' "$art/core-test-binaries.json")
while IFS= read -r path; do bins+=("$art/production-test-binaries/$(basename "$path")"); done < <(
    jq -r '.[] | select(.name == "review_api") | .exe' "$art/production-test-binaries.json")
test "${#bins[@]}" -eq 8
selected=0
for bin in "${bins[@]}"; do
    "$bin" --list obs_ r9_ > "$bin.selected-tests.txt"
    n=$(grep -c ': test$' "$bin.selected-tests.txt")
    test "$n" -gt 0
    selected=$((selected + n))
done
jq -n --argjson rounds "$rounds" --argjson selected "$selected" '{rounds:$rounds,
    selected_tests_per_round:$selected, filters:["obs_","r9_"],
    scope:"default_core_and_separate_no_demo_api; includes existing jobs_ substring matches"}' > "$art/repeat-plan.json"
for ((i=1; i<=rounds; i++)); do
    for bin in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$i" "$bin" >> "$art/repeat.log"
        "$bin" --quiet obs_ r9_ >> "$art/repeat.log" 2>&1
    done
    printf 'REPEAT_ROUND_OK %s/%s tests=%s\n' "$i" "$rounds" "$selected"
done

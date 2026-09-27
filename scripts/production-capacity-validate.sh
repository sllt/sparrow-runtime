#!/usr/bin/env bash
# Finite default-budget observations, not a replacement for release gates/soak.
# NEW_ART BASELINE_JS_PACKAGE CANDIDATE_JS_PACKAGE FROZEN DRIVER NATS [ROUNDS]
set -euo pipefail
[[ $# -eq 6 || $# -eq 7 ]]
root=$(cd "$(dirname "$0")/.." && pwd)
art=$1; before=$2; after=$3; frozen=$4; driver=$5; nats=$6; rounds=${7:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
for binary in "$before/bin/sparrow-server" "$after/bin/sparrow-server" "$driver" "$nats"; do test -x "$binary"; done
mkdir -p "$art"; art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
for package in "$before" "$after"; do (cd "$package" && sha256sum -c SHA256SUMS) >> "$art/package-verify.log"; done
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
sha256sum "$driver" "$nats" "$before/bin/sparrow-server" "$after/bin/sparrow-server" "$root/tests/capacity/plan.json" "$0" > "$art/inputs.sha256"
cp "$root/tests/capacity/plan.json" "$art/cases.json"
jq -n --argjson rounds "$rounds" '{rounds:$rounds,exact_tests:5,order:["before","after","after","before"],
    special_case:"js-idle-fast20 is candidate-only twice; old API does not support this option",
    failure_policy:"keep failures and continue other cases; no reruns or replacement samples",
    scope:"finite_loopback_not_steady_state_certification",capacity_failure_is_not_data_loss:true}' > "$art/plan.json"
LC_ALL=C sort "$root/tests/capacity/expected-tests.txt" > "$art/expected.sorted"
test "$(wc -l < "$art/expected.sorted")" -eq 5; test -z "$(uniq -d "$art/expected.sorted")"
declare -A binaries
: > "$art/discovered"
while IFS=$'\t' read -r name exe; do
    case "$name" in sparrow_formats|sparrow_control) ;; *) continue;; esac
    test -z "${binaries[$name]+present}"
    binary="$frozen/reliable-test-binaries/$(basename "$exe")"; test -x "$binary"; binaries[$name]=$binary
    "$binary" --list > "$art/list-$name"; "$binary" --list --ignored > "$art/ignored-$name"
    if grep -Eq '::capacity_[^:]*: test$' "$art/ignored-$name"; then exit 1; fi
    grep -E '::capacity_[^:]*: test$' "$art/list-$name" | sed -e 's/: test$//' -e "s/^/$name|/" >> "$art/discovered"
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
LC_ALL=C sort "$art/discovered" > "$art/discovered.sorted"
diff -u "$art/expected.sorted" "$art/discovered.sorted" > "$art/inventory-diff.log"
mkdir "$art/tests"
for ((round=1;round<=rounds;round++)); do
    while IFS='|' read -r crate name; do
        log="$art/tests/$round-$crate-${name//:/_}.log"
        "${binaries[$crate]}" --exact "$name" --test-threads=1 > "$log" 2>&1
        grep -q '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$log"
    done < "$art/expected.sorted"
done
: > "$art/results.jsonl"
failed=0
while IFS= read -r case_name; do
    order=(before after after before)
    if [[ "$case_name" == js-idle-fast20 ]]; then order=(after after); fi
    trial=0
    for variant in "${order[@]}"; do
        trial=$((trial+1)); package=$before; if [[ "$variant" == after ]]; then package=$after; fi
        out="$art/$case_name-$trial-$variant"; rc=0
        "$driver" --capacity-plan "$art/cases.json" --capacity-case "$case_name" --server-bin "$package/bin/sparrow-server" \
            --nats-server "$nats" --out "$out" > "$out.log" 2>&1 || rc=$?
        if [[ "$rc" == 0 ]]; then
            jq -e --arg variant "$variant" '.valid == true and ($variant == "before" or .credits_verified == true)' "$out/$case_name/summary.json" >/dev/null || rc=1
        fi
        printf '%s\n' "$rc" > "$out.exit"
        if [[ "$rc" == 0 ]]; then
            jq -c --arg variant "$variant" --argjson trial "$trial" '.+{variant:$variant,trial:$trial}' "$out/$case_name/summary.json" >> "$art/results.jsonl"
        else
            failed=1
            jq -nc --arg name "$case_name" --arg variant "$variant" --argjson trial "$trial" --argjson exit "$rc" \
                '{name:$name,variant:$variant,trial:$trial,valid:false,exit:$exit}' >> "$art/results.jsonl"
        fi
        printf 'CAPACITY_CASE=%s variant=%s trial=%s exit=%s\n' "$case_name" "$variant" "$trial" "$rc"
    done
done < <(jq -r '.cases[].name' "$art/cases.json")
jq -s '{all_finite_cases_valid:all(.valid),trials:length,soak:false,certified:false,capacity_requires_per_case_interpretation:true}' "$art/results.jsonl" > "$art/summary.json"
exit "$failed"

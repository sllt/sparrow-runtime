#!/usr/bin/env bash
# Execute frozen test binaries only. Does not invoke Cargo or overwrite evidence.
set -euo pipefail
art=${1:?artifact directory with core/production test manifests and binaries}
rounds=${2:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]] || exit 2
test ! -e "$art/production-repeat.log"
command -v jq >/dev/null
bins=(); selected=0
for profile in core production; do
    test -s "$art/$profile-test-binaries.json"
    jq -e 'length>0 and all(.[]; (.name|type)=="string" and (.exe|type)=="string")' "$art/$profile-test-binaries.json" >/dev/null
    (cd "$art" && sha256sum -c "$profile-test-binaries.sha256") >/dev/null
    while IFS= read -r file_path; do
        bin="$art/$profile-test-binaries/$(basename "$file_path")"
        test -x "$bin"
        "$bin" --list production_ obs_ r9_ r10_ r11_ k1_ r4_checkpoint_credit_ self_review_ > "$bin.production-selected.txt"
        count=$(grep -c ': test$' "$bin.production-selected.txt" || true)
        if [[ "$count" -gt 0 ]]; then bins+=("$bin"); selected=$((selected+count)); fi
    done < <(jq -r '.[] | select(.name=="sparrow_model" or .name=="sparrow_expr" or .name=="sparrow_io"
        or .name=="sparrow_plan" or .name=="sparrow_runtime" or .name=="sparrow_connectors"
        or .name=="sparrow_control" or .name=="review_api" or .name=="production_reference" or .name=="sparrowctl")|.exe' \
        "$art/$profile-test-binaries.json")
done
test "$selected" -gt 0
jq -n --argjson rounds "$rounds" --argjson selected "$selected" --argjson binaries "${#bins[@]}" \
    '{rounds:$rounds,tests_per_round:$selected,binaries:$binaries,filters:["production_","obs_","r9_","r10_","r11_","k1_","r4_checkpoint_credit_","self_review_"],
      scope:"frozen_default_core_and_separate_no_demo_API; deterministic_repeat_not_soak"}' > "$art/production-repeat-plan.json"
for ((round=1;round<=rounds;round++)); do
    for bin in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$bin" >> "$art/production-repeat.log"
        "$bin" --quiet production_ obs_ r9_ r10_ r11_ k1_ r4_checkpoint_credit_ self_review_ >> "$art/production-repeat.log" 2>&1
    done
done
printf 'PRODUCTION_REPEAT_OK rounds=%s tests_per_round=%s total=%s\n' "$rounds" "$selected" "$((rounds*selected))"

#!/usr/bin/env bash
# Reuse frozen executables. No compilation or production certification.
# Usage: bash scripts/production-core-b-validate.sh ART PACKAGE FROZEN DRIVER [ROUNDS] [OLD_SERVER]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; package=${2:?production package}; frozen=${3:?frozen tests}
driver=${4:?compiled Core-B process oracle}; rounds=${5:-20}; old=${6:-}
profile=${SPARROW_CORE_B_TEST_PROFILE:-reliable}
expected=${SPARROW_CORE_B_EXPECTED_TESTS:-$root/tests/core-b-process/expected-tests.txt}
[[ "$profile" == reliable || "$profile" == core ]]
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"; test -f "$expected"; test -x "$driver"; test -x "$package/bin/sparrow-server"
command -v jq >/dev/null; command -v sha256sum >/dev/null
mkdir -p "$art"; art=$(cd "$art" && pwd)
package=$(cd "$package" && pwd); frozen=$(cd "$frozen" && pwd)
(cd "$package" && sha256sum -c SHA256SUMS) > "$art/package-verify.log"
(cd "$frozen" && sha256sum -c "$profile-test-binaries.sha256") > "$art/frozen-verify.log"
cp "$expected" "$art/expected-tests.txt"; sha256sum "$expected" > "$art/expected-tests.sha256"
declare -A wanted found binary_for
count=0
while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" == *\|* && "$line" != *\|*\|* ]]
    crate=${line%%|*}; name=${line#*|}
    [[ "$crate" =~ ^[a-zA-Z0-9_]+$ && "$name" == *core_b_* && "$name" != *[[:space:]]* ]]
    case "$crate" in sparrow_plan|sparrow_runtime|sparrow_control|sparrow_server|core_b_api) ;; *)
        printf 'unsupported Core-B test target %s\n' "$crate" >&2; exit 1;; esac
    [[ -z ${wanted[$line]+x} ]] || { printf 'duplicate expected Core-B test %s\n' "$line" >&2; exit 1; }
    wanted[$line]=1; count=$((count+1))
done < "$expected"
test "$count" -gt 0
jq -e 'type=="array" and length>0 and all(.[]; (.name|type)=="string" and (.exe|type)=="string")' \
    "$frozen/$profile-test-binaries.json" >/dev/null
bins=(); i=0
while IFS=$'\t' read -r target executable; do
    crate=${target//-/_}; [[ "$crate" =~ ^[a-zA-Z0-9_]+$ ]]
    binary="$frozen/$profile-test-binaries/$(basename "$executable")"; test -x "$binary"
    raw="$art/list-$i-$crate.txt"; ignored="$art/ignored-$i-$crate.txt"
    "$binary" --list core_b_ > "$raw"
    "$binary" --list --ignored core_b_ > "$ignored"
    if grep -q ': test$' "$ignored"; then
        printf 'Core-B frozen ignored tests are not admitted by this reviewed inventory\n' >&2; exit 1
    fi
    selected=0
    while IFS= read -r name; do
        key="$crate|$name"
        [[ -z ${found[$key]+x} ]] || { printf 'duplicate discovered Core-B test %s\n' "$key" >&2; exit 1; }
        found[$key]=1; selected=1
        [[ -z ${binary_for[$crate]+x} || ${binary_for[$crate]} == "$binary" ]] || exit 1
        binary_for[$crate]=$binary
    done < <(sed -n 's/^\([^[:space:]]*core_b_[^[:space:]]*\): test$/\1/p' "$raw")
    if [[ "$selected" == 1 ]]; then bins+=("$binary"); fi
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/$profile-test-binaries.json")
[[ ${#found[@]} == "$count" ]] || { printf 'Core-B inventory count mismatch\n' >&2; exit 1; }
for key in "${!wanted[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/expected.sorted.txt"
for key in "${!found[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/discovered.sorted.txt"
diff -u "$art/expected.sorted.txt" "$art/discovered.sorted.txt" > "$art/inventory-diff.log"
jq -n --arg profile "$profile" --argjson rounds "$rounds" --argjson count "$count" \
    '{profile:$profile,rounds:$rounds,tests_per_round:$count,total:($rounds*$count),ignored:0,scope:"frozen_repeat_not_soak"}' > "$art/repeat-plan.json"
: > "$art/repeat.log"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" core_b_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
sha256sum "$driver" "$package/bin/sparrow-server" > "$art/binaries.sha256"
args=(--server-bin "$package/bin/sparrow-server" --out "$art/process")
if [[ -n "$old" ]]; then test -x "$old"; sha256sum "$old" >> "$art/binaries.sha256"; args+=(--old-server-bin "$old"); fi
"$driver" "${args[@]}" > "$art/process.log" 2>&1
jq -e 'type=="object" and .valid==true and .profile=="managed_static_lookup_restart_fresh"
    and .publication_cas==true and .invalid_publication_atomic==true and .hit_and_miss_golden==true
    and .running_binding_immutable==true and .gc_preserved_all_pipeline_revisions==true
    and .unreferenced_revision_deleted==true and .sigkill_reloaded_original_binding==true
    and .explicit_binding_update==true and .historical_pipeline_start==true
    and .aligned_rejected==true and .certified==false' "$art/process/summary.json" >/dev/null
if [[ -n "$old" ]]; then jq -e '.old_catalog_guard==true' "$art/process/summary.json" >/dev/null; fi
printf 'CORE_B_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; immutable table/Lookup/GC/restart oracle, not release approval\n' "$rounds" "$count" "$((rounds*count))"

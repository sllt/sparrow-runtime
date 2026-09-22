#!/usr/bin/env bash
# Frozen, exact-inventory B2-A tests. Does not compile or certify production.
# Usage: production-core-b2-validate.sh ART PACKAGE FROZEN DRIVER OLD_SERVER [ROUNDS]
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; package=${2:?package}; frozen=${3:?frozen tests}
driver=${4:?B2 process oracle}; old=${5:?old B1 server}; rounds=${6:-20}
profile=${SPARROW_CORE_B2_TEST_PROFILE:-reliable}
expected=${SPARROW_CORE_B2_EXPECTED_TESTS:-$root/tests/core-b2-process/expected-tests.txt}
[[ "$profile" == reliable || "$profile" == core ]]
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"; test -f "$expected"; test -x "$driver"; test -x "$old"
test -x "$package/bin/sparrow-server"
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
    [[ "$name" == *core_b2_* && "$name" != *[[:space:]]* ]]
    case "$crate" in sparrow_plan|sparrow_runtime|sparrow_control|sparrow_server) ;; *) exit 1;; esac
    [[ -z ${wanted[$line]+x} ]] || { printf 'duplicate expected B2 test %s\n' "$line" >&2; exit 1; }
    wanted[$line]=1; count=$((count+1))
done < "$expected"
test "$count" -gt 0
jq -e 'type=="array" and length>0 and all(.[]; (.name|type)=="string" and (.exe|type)=="string")' "$frozen/$profile-test-binaries.json" >/dev/null
bins=(); i=0
while IFS=$'\t' read -r target executable; do
    crate=${target//-/_}; [[ "$crate" =~ ^[a-zA-Z0-9_]+$ ]]
    binary="$frozen/$profile-test-binaries/$(basename "$executable")"; test -x "$binary"
    raw="$art/list-$i-$crate.txt"; ignored="$art/ignored-$i-$crate.txt"
    "$binary" --list core_b2_ > "$raw"
    "$binary" --list --ignored core_b2_ > "$ignored"
    if grep -q ': test$' "$ignored"; then printf 'B2 ignored tests not admitted\n' >&2; exit 1; fi
    selected=0
    while IFS= read -r name; do
        key="$crate|$name"
        [[ -z ${found[$key]+x} ]] || { printf 'duplicate discovered B2 test %s\n' "$key" >&2; exit 1; }
        found[$key]=1; selected=1
        [[ -z ${binary_for[$crate]+x} || ${binary_for[$crate]} == "$binary" ]] || exit 1
        binary_for[$crate]=$binary
    done < <(sed -n 's/^\([^[:space:]]*core_b2_[^[:space:]]*\): test$/\1/p' "$raw")
    if [[ "$selected" == 1 ]]; then bins+=("$binary"); fi
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/$profile-test-binaries.json")
[[ ${#found[@]} == "$count" ]] || { printf 'B2 inventory count mismatch\n' >&2; exit 1; }
for key in "${!wanted[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/expected.sorted.txt"
for key in "${!found[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/discovered.sorted.txt"
diff -u "$art/expected.sorted.txt" "$art/discovered.sorted.txt" > "$art/inventory-diff.log"
jq -n --arg profile "$profile" --argjson rounds "$rounds" --argjson count "$count" \
    '{profile:$profile,rounds:$rounds,tests_per_round:$count,total:($rounds*$count),ignored:0,scope:"frozen_repeat_not_soak"}' > "$art/repeat-plan.json"
: > "$art/repeat.log"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" core_b2_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
sha256sum "$driver" "$old" "$package/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --server-bin "$package/bin/sparrow-server" --old-server-bin "$old" --out "$art/process" > "$art/process.log" 2>&1
jq -e 'type=="object" and .valid==true and .profile=="static_lookup_file_aligned_v8"
    and .fixed_binding==true and .suffix_recovery==true and .http_unknown_no_early_current==true
    and .current_failure_preserved==true and .changed_binding_refused==true
    and .gc_preserved_dependency==true and .old_profile_guard==true and .new_profile_guard==true
    and .missing_dependency_api_refused==true and .certified==false' "$art/process/summary.json" >/dev/null
printf 'CORE_B2_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; File/reference checkpoint oracle, not release approval\n' "$rounds" "$count" "$((rounds*count))"

#!/usr/bin/env bash
# Repeat the reviewed K4 test inventory and run the real File -> IoT -> HTTP
# process oracle. This script never builds and is not soak/release approval.
# Usage: bash scripts/production-k4-validate.sh ART PACKAGE FROZEN DRIVER [ROUNDS] [OLD_SERVER_BIN]
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new artifact directory}
package=${2:?production package}
frozen=${3:?frozen K4 tests}
driver=${4:?compiled K4 process driver}
rounds=${5:-20}
old_server=${6:-${SPARROW_K4_OLD_SERVER_BIN:-}}
profile=${SPARROW_K4_TEST_PROFILE:-core}
expected=${SPARROW_K4_EXPECTED_TESTS:-$root/tests/k4-process/expected-tests.txt}

case "$profile" in core|reliable) ;; *) exit 2 ;; esac
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"
test -f "$expected"
test -x "$driver"
test -x "$package/bin/sparrow-server"
command -v jq >/dev/null
command -v sha256sum >/dev/null

mkdir -p "$art"
art=$(cd "$art" && pwd)
package=$(cd "$package" && pwd)
frozen=$(cd "$frozen" && pwd)
expected=$(cd "$(dirname "$expected")" && pwd)/$(basename "$expected")

(cd "$package" && sha256sum -c SHA256SUMS) > "$art/package-verify.log"
(cd "$frozen" && sha256sum -c "$profile-test-binaries.sha256") > "$art/frozen-verify.log"
cp "$expected" "$art/expected-tests.txt"
sha256sum "$expected" > "$art/expected-tests.sha256"

# The expected manifest is versioned and independent from --list discovery.
# A missing/new crate or test therefore cannot make the gate self-certify.
declare -A expected_set
expected_count=0
while IFS= read -r line || [[ -n "$line" ]]; do
    line=${line%$'\r'}
    [[ -z "$line" || "$line" == \#* ]] && continue
    if [[ "$line" != *\|* || "$line" == *\|*\|* ]]; then
        printf 'invalid expected K4 manifest line: %s\n' "$line" >&2
        exit 2
    fi
    crate=${line%%|*}
    test_name=${line#*|}
    crate=${crate//-/_}
    case "$crate" in sparrow_plan|sparrow_runtime|sparrow_control|sparrow_server) ;; *)
        printf 'unsupported expected K4 crate target: %s\n' "$crate" >&2
        exit 2
        ;;
    esac
    [[ "$test_name" == *k4_* && "$test_name" != *[[:space:]]* ]] || {
        printf 'invalid expected K4 test name: %s\n' "$test_name" >&2
        exit 2
    }
    key="$crate|$test_name"
    [[ -z "${expected_set[$key]+x}" ]] || {
        printf 'duplicate expected K4 test: %s\n' "$key" >&2
        exit 2
    }
    expected_set[$key]=1
    expected_count=$((expected_count + 1))
done < "$expected"
[[ "$expected_count" -gt 0 ]]

test -f "$frozen/$profile-test-binaries.json"
test -f "$frozen/$profile-test-binaries.sha256"
jq -e 'type=="array" and length>0 and all(.[]; (.name|type)=="string" and (.exe|type)=="string")' \
    "$frozen/$profile-test-binaries.json" >/dev/null

declare -A actual_set binary_for_crate
bins=()
list_id=0
while IFS=$'\t' read -r target executable; do
    [[ -n "$target" && -n "$executable" ]] || continue
    crate=${target//-/_}
    binary="$frozen/$profile-test-binaries/$(basename "$executable")"
    test -x "$binary"
    raw="$art/k4-list-${list_id}-${crate}.txt"
    ignored_raw="$art/k4-list-${list_id}-${crate}.ignored.txt"
    {
        printf 'crate_target=%s\nexecutable=%s\ncommand=%q --list k4_\n' "$crate" "$binary" "$binary"
        "$binary" --list k4_
    } > "$raw"
    # --list --ignored exposes tests marked #[ignore]. They cannot be part of
    # a deterministic repeat gate that intentionally does not run ignored.
    "$binary" --list --ignored k4_ > "$ignored_raw"
    if grep -q ': test$' "$ignored_raw"; then
        printf 'ignored K4 test(s) are not allowed: %s\n' "$crate" >&2
        exit 1
    fi
    found=0
    while IFS= read -r test_name; do
        [[ -n "$test_name" ]] || continue
        key="$crate|$test_name"
        [[ -z "${actual_set[$key]+x}" ]] || {
            printf 'duplicate discovered K4 test: %s\n' "$key" >&2
            exit 1
        }
        actual_set[$key]=1
        found=1
        if [[ -n "${binary_for_crate[$crate]+x}" && "${binary_for_crate[$crate]}" != "$binary" ]]; then
            printf 'K4 tests split across frozen binaries for %s\n' "$crate" >&2
            exit 1
        fi
        binary_for_crate[$crate]=$binary
    done < <(sed -n 's/^\([^[:space:]]*k4_[^[:space:]]*\): test$/\1/p' "$raw")
    [[ "$found" -eq 0 ]] || bins+=("$binary")
    list_id=$((list_id + 1))
done < <(jq -r '.[] | [.name,.exe] | @tsv' "$frozen/$profile-test-binaries.json")

[[ "${#actual_set[@]}" -eq "$expected_count" ]] || {
    printf 'K4 frozen inventory count mismatch: expected=%s discovered=%s\n' "$expected_count" "${#actual_set[@]}" >&2
    exit 1
}
for key in "${!actual_set[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/discovered-tests.txt"
for key in "${!expected_set[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/expected-tests.sorted.txt"
if ! diff -u "$art/expected-tests.sorted.txt" "$art/discovered-tests.txt" > "$art/inventory-diff.log"; then
    printf 'K4 frozen inventory differs from reviewed manifest; driver was not run\n' >&2
    exit 1
fi

binary_json=$(printf '%s\n' "${bins[@]}" | jq -Rsc 'split("\n")|map(select(length>0))')
jq -n --arg profile "$profile" --arg expected_manifest "$expected" \
    --arg expected_sha256 "$(awk '{print $1}' "$art/expected-tests.sha256")" \
    --argjson rounds "$rounds" --argjson count "$expected_count" --argjson binaries "$binary_json" \
    '{profile:$profile,expected_manifest:$expected_manifest,expected_manifest_sha256:$expected_sha256,
      tests_per_round:$count,rounds:$rounds,total:($count*$rounds),filter:"k4_",
      ignored_tests:"rejected_from_list",binaries:$binaries,
      scope:"frozen_deterministic_repeat_not_soak"}' > "$art/repeat-plan.json"

: > "$art/repeat.log"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" k4_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done

sha256sum "$driver" "$package/bin/sparrow-server" > "$art/binaries.sha256"
driver_args=(--server-bin "$package/bin/sparrow-server" --out "$art/process")
if [[ -n "$old_server" ]]; then
    test -x "$old_server"
    driver_args+=(--old-server-bin "$old_server")
fi
"$driver" "${driver_args[@]}" > "$art/process.log" 2>&1
printf 'K4_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; File/IoT checkpoint/replay/fresh/semantic/HTTP-flush oracle passed (not production certification)\n' \
    "$rounds" "$expected_count" "$((rounds * expected_count))"

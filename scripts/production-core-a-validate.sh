#!/usr/bin/env bash
# Repeat the reviewed core-A inventory (including declared broker fixtures)
# and run the real JetStream -> IoT -> HTTP fault oracle. This script never builds and is not soak/release approval.
# Usage: bash scripts/production-core-a-validate.sh ART PACKAGE FROZEN DRIVER [ROUNDS] [OLD_SERVER_BIN]
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new artifact directory}
package=${2:?production package}
frozen=${3:?frozen CORE_A tests}
driver=${4:?compiled CORE_A process driver}
rounds=${5:-20}
old_server=${6:-${SPARROW_CORE_A_OLD_SERVER_BIN:-}}
profile=reliable
expected=${SPARROW_CORE_A_EXPECTED_TESTS:-$root/tests/core-a-process/expected-tests.txt}
expected_ignored=${SPARROW_CORE_A_EXPECTED_IGNORED:-$root/tests/core-a-process/expected-ignored-tests.txt}

[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"
test -f "$expected"
test -f "$expected_ignored"
test -x "${SPARROW_NATS_SERVER:?pinned isolated broker executable required}"
[[ $("$SPARROW_NATS_SERVER" --version) == *v2.14.6* ]]
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
cp "$expected_ignored" "$art/expected-ignored-tests.txt"
sha256sum "$expected_ignored" > "$art/expected-ignored-tests.sha256"
export SPARROW_TEST_ARTIFACTS="$art/broker-fixtures"
export SPARROW_DATA_ROOTS="$SPARROW_TEST_ARTIFACTS:/tmp/sparrow"
mkdir "$SPARROW_TEST_ARTIFACTS"
: > "$art/discovered-ignored-tests.txt"

# The expected manifest is versioned and independent from --list discovery.
# A missing/new crate or test therefore cannot make the gate self-certify.
declare -A expected_set
expected_count=0
while IFS= read -r line || [[ -n "$line" ]]; do
    line=${line%$'\r'}
    [[ -z "$line" || "$line" == \#* ]] && continue
    if [[ "$line" != *\|* || "$line" == *\|*\|* ]]; then
        printf 'invalid expected CORE_A manifest line: %s\n' "$line" >&2
        exit 2
    fi
    crate=${line%%|*}
    test_name=${line#*|}
    crate=${crate//-/_}
    case "$crate" in sparrow_plan|sparrow_runtime|sparrow_control|sparrow_server) ;; *)
        printf 'unsupported expected CORE_A crate target: %s\n' "$crate" >&2
        exit 2
        ;;
    esac
    [[ "$test_name" == *core_a_* && "$test_name" != *[[:space:]]* ]] || {
        printf 'invalid expected CORE_A test name: %s\n' "$test_name" >&2
        exit 2
    }
    key="$crate|$test_name"
    [[ -z "${expected_set[$key]+x}" ]] || {
        printf 'duplicate expected CORE_A test: %s\n' "$key" >&2
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
    raw="$art/core-a-list-${list_id}-${crate}.txt"
    ignored_raw="$art/core-a-list-${list_id}-${crate}.ignored.txt"
    {
        printf 'crate_target=%s\nexecutable=%s\ncommand=%q --list core_a_\n' "$crate" "$binary" "$binary"
        "$binary" --list core_a_
    } > "$raw"
    # Broker fixtures are opt-in in cargo, but required here. Their ignored
    # classification is itself an explicit reviewed manifest, not discovery.
    "$binary" --list --ignored core_a_ > "$ignored_raw"
    while IFS= read -r test_name; do
        printf '%s|%s\n' "$crate" "$test_name" >> "$art/discovered-ignored-tests.txt"
    done < <(sed -n 's/^\([^[:space:]]*core_a_[^[:space:]]*\): test$/\1/p' "$ignored_raw")
    found=0
    while IFS= read -r test_name; do
        [[ -n "$test_name" ]] || continue
        key="$crate|$test_name"
        [[ -z "${actual_set[$key]+x}" ]] || {
            printf 'duplicate discovered CORE_A test: %s\n' "$key" >&2
            exit 1
        }
        actual_set[$key]=1
        found=1
        if [[ -n "${binary_for_crate[$crate]+x}" && "${binary_for_crate[$crate]}" != "$binary" ]]; then
            printf 'CORE_A tests split across frozen binaries for %s\n' "$crate" >&2
            exit 1
        fi
        binary_for_crate[$crate]=$binary
    done < <(sed -n 's/^\([^[:space:]]*core_a_[^[:space:]]*\): test$/\1/p' "$raw")
    [[ "$found" -eq 0 ]] || bins+=("$binary")
    list_id=$((list_id + 1))
done < <(jq -r '.[] | [.name,.exe] | @tsv' "$frozen/$profile-test-binaries.json")

[[ "${#actual_set[@]}" -eq "$expected_count" ]] || {
    printf 'CORE_A frozen inventory count mismatch: expected=%s discovered=%s\n' "$expected_count" "${#actual_set[@]}" >&2
    exit 1
}
for key in "${!actual_set[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/discovered-tests.txt"
for key in "${!expected_set[@]}"; do printf '%s\n' "$key"; done | LC_ALL=C sort > "$art/expected-tests.sorted.txt"
if ! diff -u "$art/expected-tests.sorted.txt" "$art/discovered-tests.txt" > "$art/inventory-diff.log"; then
    printf 'CORE_A frozen inventory differs from reviewed manifest; driver was not run\n' >&2
    exit 1
fi

sed '/^#/d;/^$/d' "$expected_ignored" | LC_ALL=C sort > "$art/expected-ignored.sorted.txt"
LC_ALL=C sort "$art/discovered-ignored-tests.txt" > "$art/discovered-ignored.sorted.txt"
diff -u "$art/expected-ignored.sorted.txt" "$art/discovered-ignored.sorted.txt" > "$art/ignored-inventory-diff.log"
while IFS= read -r key; do
    [[ -n "${expected_set[$key]+x}" ]] || { printf 'unknown expected ignored test: %s\n' "$key" >&2; exit 1; }
done < "$art/expected-ignored.sorted.txt"

binary_json=$(printf '%s\n' "${bins[@]}" | jq -Rsc 'split("\n")|map(select(length>0))')
jq -n --arg profile "$profile" --arg expected_manifest "$expected" \
    --arg expected_sha256 "$(awk '{print $1}' "$art/expected-tests.sha256")" \
    --argjson rounds "$rounds" --argjson count "$expected_count" --argjson binaries "$binary_json" \
    '{profile:$profile,expected_manifest:$expected_manifest,expected_manifest_sha256:$expected_sha256,
      tests_per_round:$count,rounds:$rounds,total:($count*$rounds),filter:"core_a_",
      ignored_tests:"explicit_manifest_and_include_ignored",binaries:$binaries,
      scope:"frozen_deterministic_repeat_not_soak"}' > "$art/repeat-plan.json"

: > "$art/repeat.log"
for ((round=1; round<=rounds; round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" core_a_ --include-ignored --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done

sha256sum "$driver" "$package/bin/sparrow-server" "$SPARROW_NATS_SERVER" > "$art/binaries.sha256"
driver_args=(--server-bin "$package/bin/sparrow-server" --nats-server "$SPARROW_NATS_SERVER" --out "$art/process")
if [[ -n "$old_server" ]]; then
    test -x "$old_server"
    driver_args+=(--old-server-bin "$old_server")
fi
"$driver" "${driver_args[@]}" > "$art/process.log" 2>&1
printf 'CORE_A_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; JetStream/IoT checkpoint/ACK/replay/identity/HTTP-flush oracle passed (not production certification)\n' \
    "$rounds" "$expected_count" "$((rounds * expected_count))"

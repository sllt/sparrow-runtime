#!/usr/bin/env bash
# Fail-closed contract tests for scripts/production-silence-validate.sh.
#
# Fake packages, a fake frozen manifest with stub test binaries and a stub
# process driver: no real Sparrow process, no broker, no network and no SIGKILL
# claim. The legal case shows the runner accepts the reviewed shapes; every
# broken case must be refused without a success summary or marker.
# Usage: bash tests/silence/runner-contract.sh ART
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
runner=$root/scripts/production-silence-validate.sh
[[ $# -eq 1 ]] || { printf 'usage: %s ART\n' "$0" >&2; exit 2; }
art=${1:?new evidence directory}
test ! -e "$art"
test -f "$runner"
command -v jq >/dev/null; command -v sha256sum >/dev/null
check() { "$@" || { printf 'FAIL: %s\n' "$*" >&2; exit 1; }; }
mkdir -p "$art/fixture/cases"
art=$(cd "$art" && pwd); fixture=$art/fixture; cases=$fixture/cases
cp "$root/tests/silence/stub-driver.sh" "$fixture/stub-driver"; chmod 0755 "$fixture/stub-driver"
stub=$fixture/stub-driver

fake_bin() { printf '#!/usr/bin/env bash\nexit 0\n' > "$1"; chmod 0755 "$1"; }
fake_package() {
    mkdir -p "$1/bin"
    fake_bin "$1/bin/sparrow-server"
    (cd "$1" && sha256sum bin/sparrow-server > SHA256SUMS)
    chmod 0644 "$1/SHA256SUMS"
}
fake_package "$fixture/default-package"
fake_package "$fixture/js-package"
fake_bin "$fixture/old-server"; fake_bin "$fixture/nats-server"
default_pkg=$fixture/default-package; js_pkg=$fixture/js-package
old=$fixture/old-server; nats=$fixture/nats-server
cp -R "$fixture/default-package" "$fixture/broken-package"
printf '%s  bin/sparrow-server\n' '0000000000000000000000000000000000000000000000000000000000000000' \
    >> "$fixture/broken-package/SHA256SUMS"

# Frozen manifest plus stub test binaries. The stubs come from the reviewed
# tests/silence/stub-test-binary.sh fixture, copied once per crate name; they
# answer --list from the mock discovery file and print a cargo-shaped result
# line when a test runs, so an exit code alone cannot stand in for an executed
# test.
frozen=$fixture/frozen
mkdir -p "$frozen/reliable-test-binaries"
for crate in sparrow_plan sparrow_runtime sparrow_control; do
    cp "$root/tests/silence/stub-test-binary.sh" "$frozen/reliable-test-binaries/$crate"
    chmod 0755 "$frozen/reliable-test-binaries/$crate"
done
(cd "$frozen" && sha256sum reliable-test-binaries/* > reliable-test-binaries.sha256)
jq -n --arg base "$frozen/reliable-test-binaries" \
    '["sparrow_plan","sparrow_runtime","sparrow_control"]|map({name:.,exe:($base+"/"+.)})' \
    > "$frozen/reliable-test-binaries.json"
cp "$root/tests/silence/expected-tests.txt" "$fixture/mock-actual.txt"

mode=ok; fail_package=; inventory=exact; mock_mode=ok
run_case() { # name -> exit code on stdout, log kept under ART
    local child=$cases/$1 log=$cases/$1.log rc=0
    case $inventory in
        exact) cp "$fixture/mock-actual.txt" "$fixture/actual.txt" ;;
        missing) grep -v '^sparrow_plan|silence_tests::silence_input_bound_stays_conservative$' \
                     "$fixture/mock-actual.txt" > "$fixture/actual.txt" ;;
        extra) cp "$fixture/mock-actual.txt" "$fixture/actual.txt"
               printf '%s\n' 'sparrow_runtime|silence_tests::silence_inventory_extra_fixture' >> "$fixture/actual.txt" ;;
    esac
    SPARROW_STUB_MODE=$mode SPARROW_STUB_FAIL_PACKAGE=$fail_package \
    SPARROW_SILENCE_MOCK_ACTUAL=$fixture/actual.txt SPARROW_SILENCE_MOCK_MODE=$mock_mode \
        "$runner" "$child" "$default_pkg" "$js_pkg" "$frozen" "$stub" "$old" "$nats" 1 \
        > "$log" 2>&1 || rc=$?
    printf '%s mode=%s inventory=%s mock=%s rc=%s\n' "$1" "$mode" "$inventory" "$mock_mode" "$rc" \
        >> "$cases/results.txt"
    printf '%s\n' "$rc"
}
expect_fail() {
    local name=$1 rc
    rc=$(run_case "$name")
    if [[ $rc -eq 0 ]]; then printf 'FAIL %s: runner accepted a broken fixture\n' "$name" >&2; exit 1; fi
    test ! -e "$cases/$name/summary.json" ||
        { printf 'FAIL %s: runner wrote a success summary\n' "$name" >&2; exit 1; }
    if grep -q '^SILENCE_FROZEN_AND_PROCESS_OK$' "$cases/$name.log"; then
        printf 'FAIL %s: runner printed the success marker\n' "$name" >&2; exit 1
    fi
    printf 'REFUSED %s\n' "$name"
}

rc=$(run_case legal)
if [[ $rc -ne 0 ]]; then
    printf 'FAIL legal: runner rejected a valid fixture (rc=%s)\n' "$rc" >&2
    tail -n 40 "$cases/legal.log" >&2
    exit 1
fi
check grep -q '^SILENCE_FROZEN_AND_PROCESS_OK$' "$cases/legal.log"
check test "$(sed -n '1p' "$cases/legal/exit")" -eq 0
check test -f "$cases/legal/default-silence/summary.json"
check test -f "$cases/legal/jetstream-silence/summary.json"
check jq -e '(.valid == true) and (.transport_cases == 6) and (.crash_scenarios == 30)
    and (.snapshot_versions == [23,24]) and (.actual_sigkill == true)
    and (.process_outage_only == true) and (.broker_outage_tested == false)
    and (.exactly_once_claimed == false) and (.soak == false) and (.certified == false)
    and (.source_scope_limited == true)' "$cases/legal/summary.json" >/dev/null
check jq -e '(.valid == true) and (.transport_cases == 2) and (.crash_scenarios == 10)
    and (.snapshot_versions == [23]) and (.file_only == true)
    and ((.old_profile_guards | keys) == ["file"])' \
    "$cases/legal/default-silence/summary.json" >/dev/null
check jq -e '(.valid == true) and (.transport_cases == 4) and (.crash_scenarios == 20)
    and (.snapshot_versions == [23,24]) and (.file_only == false)
    and ((.old_profile_guards | keys) == ["file","jetstream"])' \
    "$cases/legal/jetstream-silence/summary.json" >/dev/null
check jq -e '(.valid == true) and (.transport == "jetstream") and (.mode == "registered")
    and (.snapshot_version == 24) and (.state_kind == 12) and (.crashes == 5)
    and (.held_replay_identical == true) and (.committed_restart_no_repeat == true)
    and (.downtime_paused == true)' \
    "$cases/legal/jetstream-silence/jetstream-registered/summary.json" >/dev/null
count=$(wc -l < "$cases/legal/expected.sorted.txt")
check test "$count" -eq "$(wc -l < "$root/tests/silence/expected-tests.txt")"
check test "$(wc -l < "$cases/legal/test-logs/r1-sparrow_plan-silence_tests--silence_input_bound_stays_conservative.log")" -gt 0
printf 'ACCEPTED legal\n'

mode=empty-guards; expect_fail empty-guards
mode=guard; expect_fail missing-guard
mode=missing-crash; expect_fail short-crash-count
mode=wrong-version; expect_fail wrong-version
mode=wrong-kind; expect_fail wrong-state-kind
mode=false-sigkill; expect_fail false-sigkill
mode=false-replay; expect_fail false-held-replay
mode=false-commit; expect_fail false-committed-restart
mode=false-downtime; expect_fail false-downtime
mode=broker-outage-claim; expect_fail broker-outage-claim
mode=missing-subscenario; expect_fail missing-subscenario
mode=missing-marker; expect_fail missing-marker
mode=driver-fail; expect_fail driver-nonzero
mode=ok; fail_package=js-package; expect_fail second-package-bad
check test -f "$cases/second-package-bad/default-silence/summary.json"
check test ! -e "$cases/second-package-bad/jetstream-silence/summary.json"
fail_package=
inventory=missing; expect_fail inventory-missing
inventory=extra; expect_fail inventory-extra
inventory=exact; mock_mode=zero-tests; expect_fail zero-tests-exit-zero
mock_mode=ok
default_pkg=$fixture/broken-package; expect_fail corrupt-checksum
default_pkg=$fixture/default-package

# Artifact reuse must be rejected without touching the previous run's evidence.
before=$(sha256sum "$cases/legal/summary.json" "$cases/legal/exit")
rc=0
SPARROW_STUB_MODE=ok SPARROW_SILENCE_MOCK_ACTUAL=$fixture/mock-actual.txt SPARROW_SILENCE_MOCK_MODE=ok \
    "$runner" "$cases/legal" "$default_pkg" "$js_pkg" "$frozen" "$stub" "$old" "$nats" 1 \
    > "$cases/reuse.log" 2>&1 || rc=$?
[[ $rc -ne 0 ]] || { printf 'FAIL reuse: runner reused an existing ART\n' >&2; exit 1; }
[[ "$before" == "$(sha256sum "$cases/legal/summary.json" "$cases/legal/exit")" ]] ||
    { printf 'FAIL reuse: previous evidence was overwritten\n' >&2; exit 1; }
if grep -q '^SILENCE_FROZEN_AND_PROCESS_OK$' "$cases/reuse.log"; then
    printf 'FAIL reuse: printed the success marker\n' >&2; exit 1
fi
printf 'REFUSED reuse-existing-art\n'

printf 'RUNNER_CONTRACT_TESTS_OK\n'
printf 'contract-only: stub driver, stub test binaries and fake packages; no real Sparrow process, broker, network or SIGKILL claim\n'

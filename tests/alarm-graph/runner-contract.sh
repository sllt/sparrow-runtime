#!/usr/bin/env bash
# Fail-closed contract tests for scripts/production-alarm-graph-validate.sh.
# Fake packages and a stub driver only: no real Sparrow process, no network, and
# no claim of real graph SIGKILL coverage. The legal case shows the runner
# accepts the reviewed summary shape; the broken cases must refuse to pass.
# Usage: bash tests/alarm-graph/runner-contract.sh ART
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
runner=$root/scripts/production-alarm-graph-validate.sh
[[ $# -eq 1 ]] || { printf 'usage: %s ART\n' "$0" >&2; exit 2; }
art=${1:?new evidence directory}
test ! -e "$art"
test -f "$runner"
command -v jq >/dev/null; command -v sha256sum >/dev/null
check() { "$@" || { printf 'FAIL: %s\n' "$*" >&2; exit 1; }; }
mkdir -p "$art/fixture/cases"
art=$(cd "$art" && pwd); fixture=$art/fixture; cases=$fixture/cases
cp "$root/tests/alarm-graph/stub-driver.sh" "$fixture/stub-driver"; chmod 0755 "$fixture/stub-driver"
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

mode=ok; fail_package=
run_case() { # name -> exit code on stdout, log kept under ART
    local child=$cases/$1 log=$cases/$1.log rc=0
    SPARROW_STUB_MODE=$mode SPARROW_STUB_FAIL_PACKAGE=$fail_package \
        "$runner" "$child" "$default_pkg" "$js_pkg" "$stub" "$old" "$nats" > "$log" 2>&1 || rc=$?
    printf '%s mode=%s fail_package=%s rc=%s\n' "$1" "$mode" "$fail_package" "$rc" >> "$cases/results.txt"
    printf '%s\n' "$rc"
}
expect_fail() {
    local name=$1 rc
    rc=$(run_case "$name")
    if [[ $rc -eq 0 ]]; then printf 'FAIL %s: runner accepted a broken fixture\n' "$name" >&2; exit 1; fi
    test ! -e "$cases/$name/summary.json" ||
        { printf 'FAIL %s: runner wrote a success summary\n' "$name" >&2; exit 1; }
    if grep -q '^ALARM_GRAPH_PROCESS_OK$' "$cases/$name.log"; then
        printf 'FAIL %s: runner printed the success marker\n' "$name" >&2; exit 1
    fi
    printf 'REFUSED %s\n' "$name"
}

mode=ok
rc=$(run_case legal)
[[ $rc -eq 0 ]] || { printf 'FAIL legal: runner rejected a valid stub shape (rc=%s)\n' "$rc" >&2; exit 1; }
check grep -q '^ALARM_GRAPH_PROCESS_OK$' "$cases/legal.log"
check test "$(sed -n '1p' "$cases/legal/exit")" -eq 0
check test -f "$cases/legal/default-graph/summary.json"
check test -f "$cases/legal/jetstream-graph/summary.json"
check jq -e '(.valid == true) and (.process_scenarios == 4) and (.crash_scenarios == 4)
    and (.per_package_scenarios == 2) and (.snapshot_versions == [22])
    and (.graph_process_sigkill == true) and (.source_scope == "file")
    and (.exactly_once_claimed == false) and (.soak == false) and (.certified == false)' \
    "$cases/legal/summary.json" >/dev/null
printf 'ACCEPTED legal\n'

mode=empty-guards; expect_fail empty-guards
mode=bad-version; expect_fail bad-version
mode=missing-sigkill; expect_fail missing-sigkill
mode=missing-episode; expect_fail missing-episode
mode=driver-fail; expect_fail driver-nonzero
mode=ok; fail_package=js-package; expect_fail second-package-bad
check test -f "$cases/second-package-bad/default-graph/summary.json"
check test ! -e "$cases/second-package-bad/jetstream-graph/summary.json"
fail_package=
default_pkg=$fixture/broken-package; expect_fail corrupt-checksum
default_pkg=$fixture/default-package

# Artifact reuse must be rejected without touching the previous run's evidence.
before=$(sha256sum "$cases/legal/summary.json" "$cases/legal/exit")
rc=0
SPARROW_STUB_MODE=ok "$runner" "$cases/legal" "$default_pkg" "$js_pkg" "$stub" "$old" "$nats" \
    > "$cases/reuse.log" 2>&1 || rc=$?
[[ $rc -ne 0 ]] || { printf 'FAIL reuse: runner reused an existing ART\n' >&2; exit 1; }
[[ "$before" == "$(sha256sum "$cases/legal/summary.json" "$cases/legal/exit")" ]] ||
    { printf 'FAIL reuse: previous evidence was overwritten\n' >&2; exit 1; }
if grep -q '^ALARM_GRAPH_PROCESS_OK$' "$cases/reuse.log"; then
    printf 'FAIL reuse: printed the success marker\n' >&2; exit 1
fi
printf 'REFUSED reuse-existing-art\n'

printf 'RUNNER_CONTRACT_TESTS_OK\n'
printf 'contract-only: stub driver and fake packages; no real Sparrow process, network, or SIGKILL claim\n'

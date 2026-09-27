#!/usr/bin/env bash
# Orchestration contract only: fake executables, no real server or SIGKILL.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
art=${1:?new artifact directory}; test ! -e "$art"; mkdir -p "$art/fixture"
art=$(cd "$art" && pwd); fixture=$art/fixture
export RESAMPLE_EXPECTED="$root/tests/resample/expected-tests.txt"
fake="$fixture/driver"
cp "$root/tests/resample/stub-driver.sh" "$fake"; chmod 0755 "$fake"
for package in default jetstream; do
    mkdir -p "$fixture/$package/bin"
    cp "$fake" "$fixture/$package/bin/sparrow-server"
    (cd "$fixture/$package" && sha256sum bin/sparrow-server > SHA256SUMS)
done
mkdir -p "$fixture/frozen/reliable-test-binaries"
for crate in sparrow_plan sparrow_runtime sparrow_control; do
    cp "$fake" "$fixture/frozen/reliable-test-binaries/$crate"
done
jq -n '["sparrow_plan","sparrow_runtime","sparrow_control"]|map({name:.,exe:.})' > "$fixture/frozen/reliable-test-binaries.json"
(cd "$fixture/frozen" && sha256sum reliable-test-binaries/* > reliable-test-binaries.sha256)
run() {
    RESAMPLE_STUB_MODE=$1 bash "$root/scripts/production-resample-validate.sh" "$art/$2" \
        "$fixture/default" "$fixture/jetstream" "$fixture/frozen" "$fake" "$fake" "$fake" 1 > "$art/$2.log" 2>&1
}
run ok legal
jq -e '.valid == true and .tests_per_round == 24 and .crash_scenarios == 30' "$art/legal/summary.json" >/dev/null
for mode in empty-guards wrong-version missing-replay missing-wait wrong-crashes driver-fail zero-tests missing-case; do
    if run "$mode" "$mode"; then printf 'runner accepted invalid fixture: %s\n' "$mode" >&2; exit 1; fi
    test ! -e "$art/$mode/summary.json"
    if grep -qx RESAMPLE_VALIDATION_OK "$art/$mode.log"; then exit 1; fi
    printf 'REFUSED %s\n' "$mode"
done
before=$(sha256sum "$art/legal/summary.json" "$art/legal/exit")
if run ok legal; then printf 'runner accepted artifact reuse\n' >&2; exit 1; fi
test "$before" = "$(sha256sum "$art/legal/summary.json" "$art/legal/exit")"
printf 'RUNNER_CONTRACT_OK: one legal stub, nine refusals; no real process or crash evidence\n'

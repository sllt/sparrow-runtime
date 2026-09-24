#!/usr/bin/env bash
# Stub frozen test binary used only by tests/silence/runner-contract.sh.
#
# It answers `--list` from the mock discovery file and prints a cargo-shaped
# result line when a test runs, so the runner cannot accept an exit code that
# came from a filter matching nothing. The contract copies this source into
# three crate-named stubs; it is a fixture, never a build product.
#
# Environment: SPARROW_SILENCE_MOCK_ACTUAL (discovery lines), and
# SPARROW_SILENCE_MOCK_MODE=zero-tests to report zero executed tests.
set -euo pipefail
crate=$(basename "$0")
listing=0; ignored=0; name=; previous=
for arg in "$@"; do
    [[ $arg == --list ]] && listing=1
    [[ $arg == --ignored ]] && ignored=1
    [[ $previous == --exact ]] && name=$arg
    previous=$arg
done
if (( listing )); then
    (( ignored )) && exit 0
    while IFS='|' read -r mock_crate mock_name; do
        [[ $mock_crate == "$crate" ]] && printf '%s: test\n' "$mock_name"
    done < "${SPARROW_SILENCE_MOCK_ACTUAL:?mock discovery file required}"
    exit 0
fi
printf 'running 1 test\ntest %s ... ok\n\n' "$name"
if [[ ${SPARROW_SILENCE_MOCK_MODE:-ok} == zero-tests ]]; then
    printf 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n'
else
    printf 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n'
fi

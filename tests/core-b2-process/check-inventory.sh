#!/usr/bin/env bash
# Exercise the exact B2-A production inventory gate with shell fixtures only.
# No compilation, server, broker, or network access is used here.
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
art=${1:?new inventory-test artifact directory}
test ! -e "$art"
mkdir -p "$art"
art=$(cd "$art" && pwd)
expected_base="$repo/tests/core-b2-process/expected-tests.txt"

for scenario in valid1round zero missing extra duplicate ignored unknowncrate duplicatebin pseudo_bool_summary; do
    work="$art/$scenario"
    package="$work/package"
    frozen="$work/frozen"
    mkdir -p "$package/bin" "$frozen/reliable-test-binaries"

    printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$package/bin/sparrow-server"
    chmod +x "$package/bin/sparrow-server"
    (cd "$package"; sha256sum bin/sparrow-server > SHA256SUMS)

    printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$work/old-server"
    chmod +x "$work/old-server"

    cp "$expected_base" "$work/expected.txt"
    cp "$expected_base" "$work/actual.txt"
    case "$scenario" in
        zero)
            printf '%s\n' '[]' > "$frozen/reliable-test-binaries.json"
            ;;
        missing)
            grep -v '^sparrow_plan|' "$work/actual.txt" > "$work/actual.tmp"
            mv "$work/actual.tmp" "$work/actual.txt"
            ;;
        extra)
            printf '%s\n' 'sparrow_runtime|core_b2_tests::core_b2_inventory_extra_fixture' >> "$work/actual.txt"
            ;;
        duplicate)
            sed -n '/^sparrow_plan|/p' "$work/expected.txt" | sed -n '1p' >> "$work/actual.txt"
            ;;
        unknowncrate)
            printf '%s\n' 'unreviewed_target|core_b2_tests::core_b2_unknown_fixture' > "$work/expected.txt"
            ;;
        pseudo_bool_summary)
            export CORE_B2_MOCK_SUMMARY=pseudo_bool
            ;;
        *)
            ;;
    esac

    for crate in sparrow_plan sparrow_runtime sparrow_control; do
        binary="$frozen/reliable-test-binaries/$crate"
        printf '%s\n' \
            '#!/usr/bin/env bash' \
            'set -euo pipefail' \
            'crate=$(basename "$0")' \
            'if [[ "$*" == *--list* ]]; then' \
            '  if [[ "$*" == *--ignored* ]]; then' \
            '    if [[ "${CORE_B2_MOCK_IGNORED:-0}" == 1 ]]; then printf "%s: test\n" "core_b2_tests::core_b2_verified_dependency_requires_digest_and_crc"; fi' \
            '  else' \
            '    while IFS="|" read -r expected_crate test_name; do [[ "$expected_crate" == "$crate" ]] && printf "%s: test\n" "$test_name"; done < "$CORE_B2_MOCK_ACTUAL"' \
            '  fi' \
            '  exit 0' \
            'fi' \
            'exit 0' > "$binary"
        chmod +x "$binary"
    done

    if [[ "$scenario" != zero ]]; then
        jq -n \
            --arg plan "$frozen/reliable-test-binaries/sparrow_plan" \
            --arg runtime "$frozen/reliable-test-binaries/sparrow_runtime" \
            --arg control "$frozen/reliable-test-binaries/sparrow_control" \
            '[{name:"sparrow_plan",exe:$plan},{name:"sparrow_runtime",exe:$runtime},{name:"sparrow_control",exe:$control}]' \
            > "$frozen/reliable-test-binaries.json"
    fi
    if [[ "$scenario" == duplicatebin ]]; then
        jq '. + [.[0]]' "$frozen/reliable-test-binaries.json" > "$frozen/reliable-test-binaries.tmp"
        mv "$frozen/reliable-test-binaries.tmp" "$frozen/reliable-test-binaries.json"
    fi
    (cd "$frozen"; sha256sum reliable-test-binaries/* > reliable-test-binaries.sha256)

    printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' \
        'out=' \
        'while [[ $# -gt 0 ]]; do' \
        '  case "$1" in --out) out=$2; shift 2;; *) shift;; esac' \
        'done' \
        'test -n "$out"' \
        'mkdir -p "$out"' \
        'touch "$CORE_B2_MOCK_DRIVER_MARKER"' \
        'if [[ "${CORE_B2_MOCK_SUMMARY:-valid}" == pseudo_bool ]]; then' \
        '  printf "%s\n" '\''{"valid":"true","profile":"static_lookup_file_aligned_v8","fixed_binding":true,"suffix_recovery":true,"http_unknown_no_early_current":true,"current_failure_preserved":true,"changed_binding_refused":true,"gc_preserved_dependency":true,"old_profile_guard":true,"new_profile_guard":true,"missing_dependency_api_refused":true,"certified":false}'\'' > "$out/summary.json"' \
        'else' \
        '  printf "%s\n" '\''{"valid":true,"profile":"static_lookup_file_aligned_v8","fixed_binding":true,"suffix_recovery":true,"http_unknown_no_early_current":true,"current_failure_preserved":true,"changed_binding_refused":true,"gc_preserved_dependency":true,"old_profile_guard":true,"new_profile_guard":true,"missing_dependency_api_refused":true,"certified":false}'\'' > "$out/summary.json"' \
        'fi' > "$work/driver"
    chmod +x "$work/driver"

    result=0
    export CORE_B2_MOCK_ACTUAL="$work/actual.txt"
    export CORE_B2_MOCK_IGNORED=0
    export CORE_B2_MOCK_DRIVER_MARKER="$work/driver-ran"
    if [[ "$scenario" == ignored ]]; then
        export CORE_B2_MOCK_IGNORED=1
    fi
    if [[ "$scenario" != pseudo_bool_summary ]]; then
        unset CORE_B2_MOCK_SUMMARY || true
    fi
    SPARROW_CORE_B2_TEST_PROFILE=reliable \
        SPARROW_CORE_B2_EXPECTED_TESTS="$work/expected.txt" \
        bash "$repo/scripts/production-core-b2-validate.sh" \
            "$work/result" "$package" "$frozen" "$work/driver" "$work/old-server" 1 \
            > "$work/run.log" 2>&1 || result=$?

    if [[ "$scenario" == valid1round ]]; then
        test "$result" = 0
        test -f "$CORE_B2_MOCK_DRIVER_MARKER"
    elif [[ "$scenario" == pseudo_bool_summary ]]; then
        test "$result" != 0
        test -f "$CORE_B2_MOCK_DRIVER_MARKER"
    else
        test "$result" != 0
        test ! -e "$CORE_B2_MOCK_DRIVER_MARKER"
    fi
    printf 'CORE_B2_INVENTORY_SELFTEST_OK %s exit=%s\n' "$scenario" "$result"
done

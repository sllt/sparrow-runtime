#!/usr/bin/env bash
# Negative fixtures for the no-build validator. These are not product tests.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
art=${1:?new self-test artifact directory}; test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
for scenario in exact missing renamed extra duplicate unexpected_ignored duplicate_expected extra_crate invalid_crate malformed_line existing_artifact string_summary false_oracle; do
    work="$art/$scenario"; mkdir -p "$work/package/bin" "$work/frozen/core-test-binaries"
    printf '#!/usr/bin/env bash\nexit 0\n' > "$work/package/bin/sparrow-server"
    chmod +x "$work/package/bin/sparrow-server"
    (cd "$work/package"; sha256sum bin/sparrow-server > SHA256SUMS)
    printf 'sparrow_runtime|core_b_expected\n' > "$work/expected.txt"
    if [[ "$scenario" == invalid_crate ]]; then printf 'unreviewed_target|core_b_expected\n' > "$work/expected.txt"; fi
    if [[ "$scenario" == malformed_line ]]; then printf 'sparrow_runtime|core_b_expected|extra\n' > "$work/expected.txt"; fi
    if [[ "$scenario" == duplicate_expected ]]; then printf 'sparrow_runtime|core_b_expected\n' >> "$work/expected.txt"; fi
    name=core_b_expected; [[ "$scenario" != renamed ]] || name=core_b_renamed
    binary="$work/frozen/core-test-binaries/runtime-test"
    {
        printf '#!/usr/bin/env bash\nset -euo pipefail\n'
        printf 'if [[ " $* " == *" --ignored "* ]]; then\n'
        if [[ "$scenario" == unexpected_ignored ]]; then printf '  printf "core_b_expected: test\\n"\n'; fi
        printf '  exit 0\nfi\nif [[ " $* " == *" --list "* ]]; then\n'
        if [[ "$scenario" != missing ]]; then printf '  printf "%s: test\\n"\n' "$name"; fi
        if [[ "$scenario" == duplicate ]]; then printf '  printf "core_b_expected: test\\n"\n'; fi
        if [[ "$scenario" == extra ]]; then printf '  printf "core_b_extra: test\\n"\n'; fi
        printf '  exit 0\nfi\nprintf "test result: ok. 1 passed; 0 failed\\n"\n'
    } > "$binary"
    chmod +x "$binary"
    jq -n --arg exe "$binary" '[{name:"sparrow_runtime",exe:$exe}]' > "$work/frozen/core-test-binaries.json"
    if [[ "$scenario" == extra_crate ]]; then
        cp "$binary" "$work/frozen/core-test-binaries/extra-test"
        jq --arg exe "$work/frozen/core-test-binaries/extra-test" '.+[{name:"core_b_api",exe:$exe}]' \
            "$work/frozen/core-test-binaries.json" > "$work/frozen/more.json"
        mv "$work/frozen/more.json" "$work/frozen/core-test-binaries.json"
    fi
    (cd "$work/frozen"; sha256sum core-test-binaries/* > core-test-binaries.sha256)
    {
        printf '#!/usr/bin/env bash\nset -euo pipefail\nout=\nwhile [[ $# -gt 0 ]]; do\n'
        printf '  case "$1" in --out) out=$2; shift 2;; *) shift;; esac\ndone\n'
        valid=true; [[ "$scenario" != string_summary ]] || valid='"false"'
        oracle=true; [[ "$scenario" != false_oracle ]] || oracle=false
        summary="{\"valid\":$valid,\"profile\":\"managed_static_lookup_restart_fresh\",\"publication_cas\":true,\"invalid_publication_atomic\":$oracle,\"hit_and_miss_golden\":true,\"running_binding_immutable\":true,\"gc_preserved_all_pipeline_revisions\":true,\"unreferenced_revision_deleted\":true,\"sigkill_reloaded_original_binding\":true,\"explicit_binding_update\":true,\"historical_pipeline_start\":true,\"aligned_rejected\":true,\"certified\":false}"
        printf 'mkdir "$out"\nprintf '\''%%s\\n'\'' %q > "$out/summary.json"\n' "$summary"
        printf 'touch %q\n' "$work/driver-ran"
    } > "$work/driver"
    chmod +x "$work/driver"
    if [[ "$scenario" == existing_artifact ]]; then mkdir "$work/result"; fi
    result=0
    SPARROW_CORE_B_TEST_PROFILE=core SPARROW_CORE_B_EXPECTED_TESTS="$work/expected.txt" \
        bash "$root/scripts/production-core-b-validate.sh" "$work/result" "$work/package" "$work/frozen" "$work/driver" 1 \
        > "$work/run.log" 2>&1 || result=$?
    if [[ "$scenario" == exact ]]; then test "$result" = 0; test -f "$work/driver-ran"
    elif [[ "$scenario" == string_summary || "$scenario" == false_oracle ]]; then test "$result" != 0; test -e "$work/driver-ran"
    else test "$result" != 0; test ! -e "$work/driver-ran"; fi
    printf 'CORE_B_INVENTORY_SELFTEST_OK %s\n' "$scenario"
done

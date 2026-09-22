#!/usr/bin/env bash
# Exercise the gate without compiling or launching a real service.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
art=${1:?new inventory-test artifact directory}
test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
for scenario in valid zero missing extra duplicate ignored unknowncrate duplicatebin pseudo_bool_summary; do
    work="$art/$scenario"; package="$work/package"; frozen="$work/frozen"
    mkdir -p "$package/bin" "$frozen/reliable-test-binaries"
    printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$package/bin/sparrow-server"
    chmod +x "$package/bin/sparrow-server"
    cp "$package/bin/sparrow-server" "$work/old-server"
    cp "$package/bin/sparrow-server" "$work/nats-server"
    (cd "$package"; sha256sum bin/sparrow-server > SHA256SUMS)
    cp "$repo/tests/k1-k4-reference-process/expected-tests.txt" "$work/expected.txt"
    cp "$work/expected.txt" "$work/actual.txt"
    case "$scenario" in
        zero) printf '%s\n' '[]' > "$frozen/reliable-test-binaries.json";;
        missing) grep -v '^sparrow_plan|' "$work/actual.txt" > "$work/actual.tmp"; mv "$work/actual.tmp" "$work/actual.txt";;
        extra) printf '%s\n' 'sparrow_runtime|completion_inventory_extra' >> "$work/actual.txt";;
        duplicate) sed -n '/^sparrow_plan|/p' "$work/expected.txt" | sed -n '1p' >> "$work/actual.txt";;
        unknowncrate) printf '%s\n' 'unknown_target|completion_unknown_fixture' > "$work/expected.txt";;
    esac
    for crate in sparrow_plan sparrow_runtime sparrow_control; do
        binary="$frozen/reliable-test-binaries/$crate"
        printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' \
            'crate=$(basename "$0")' \
            'if [[ "$*" == *--list* ]]; then' \
            '  if [[ "$*" == *--ignored* ]]; then' \
            '    if [[ "$COMPLETION_MOCK_SCENARIO" == ignored ]]; then printf "%s: test\n" "completion_ignored_fixture"; fi' \
            '  else' \
            '    while IFS="|" read -r expected_crate test_name; do [[ "$expected_crate" == "$crate" ]] && printf "%s: test\n" "$test_name"; done < "$COMPLETION_MOCK_ACTUAL"' \
            '  fi' \
            'fi' 'exit 0' > "$binary"
        chmod +x "$binary"
    done
    if [[ "$scenario" != zero ]]; then
        jq -n --arg base "$frozen/reliable-test-binaries" \
            '["sparrow_plan","sparrow_runtime","sparrow_control"]|map({name:.,exe:($base+"/"+.)})' \
            > "$frozen/reliable-test-binaries.json"
    fi
    if [[ "$scenario" == duplicatebin ]]; then
        jq '.+[.[0]]' "$frozen/reliable-test-binaries.json" > "$frozen/tmp.json"
        mv "$frozen/tmp.json" "$frozen/reliable-test-binaries.json"
    fi
    (cd "$frozen"; sha256sum reliable-test-binaries/* > reliable-test-binaries.sha256)
    jq -n '{valid:true,ttl_micros:0,
        profile9_file_lookup_count_iot:true,profile9_file_count_lookup:true,
        profile10_jetstream_lookup_count:true,profile10_jetstream_lookup_iot:true,
        profile11_file_dag_branch:true,profile11_file_dag_union:true,
        profile11_file_dag_count_hysteresis:true,
        profile12_hysteresis_file:true,profile13_hysteresis_jetstream:true,
        snapshot_versions:{profile9:9,profile10:10,profile11:11,profile12:12,profile13:13},
        exactly_once_claimed:false,at_least_once_output_replay_explicit:true,certified:false}
        | . as $s
        | reduce ["old_profile8_v9_guard","old_profile8_v10_guard","old_profile8_v11_guard","new_profile8_v10_guard","old_profile8_v12_guard","old_profile8_v13_guard"][] as $k
          ($s; .[$k]={checked:true,history_preserved:true,current_preserved:true,output_preserved:true})' > "$work/mock-summary.json"
    if [[ "$scenario" == pseudo_bool_summary ]]; then
        jq '.valid="true"' "$work/mock-summary.json" > "$work/tmp.json"
        mv "$work/tmp.json" "$work/mock-summary.json"
    fi
    printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' \
        'out=' 'while [[ $# -gt 0 ]]; do' \
        'case "$1" in --out) out=$2; shift 2;; *) shift;; esac' 'done' \
        'test -n "$out"; mkdir -p "$out"; touch "$COMPLETION_MOCK_DRIVER_MARKER"' \
        'cp "$COMPLETION_MOCK_SUMMARY" "$out/summary.json"' > "$work/driver"
    chmod +x "$work/driver"
    export COMPLETION_MOCK_ACTUAL="$work/actual.txt" COMPLETION_MOCK_SCENARIO="$scenario"
    export COMPLETION_MOCK_DRIVER_MARKER="$work/driver-ran" COMPLETION_MOCK_SUMMARY="$work/mock-summary.json"
    result=0
    SPARROW_COMPLETION_EXPECTED_TESTS="$work/expected.txt" \
        bash "$repo/scripts/production-k1-k4-completion-validate.sh" \
        "$work/result" "$package" "$frozen" "$work/driver" "$work/old-server" "$work/nats-server" 1 \
        > "$work/run.log" 2>&1 || result=$?
    if [[ "$scenario" == valid ]]; then
        test "$result" = 0; test -f "$COMPLETION_MOCK_DRIVER_MARKER"
    elif [[ "$scenario" == pseudo_bool_summary ]]; then
        test "$result" != 0; test -f "$COMPLETION_MOCK_DRIVER_MARKER"
    else
        test "$result" != 0; test ! -e "$COMPLETION_MOCK_DRIVER_MARKER"
    fi
    printf 'COMPLETION_INVENTORY_SELFTEST_OK %s exit=%s\n' "$scenario" "$result"
done

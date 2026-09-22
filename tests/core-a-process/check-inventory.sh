#!/usr/bin/env bash
# Exercise exact inventory and ignored-classification gates without compiling
# or launching a real broker. Fixture executables never access the network.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
art=${1:?new gate artifact directory}
test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
package="$art/package"; frozen="$art/frozen"
mkdir -p "$package/bin" "$frozen/reliable-test-binaries"
printf '#!/usr/bin/env bash\nexit 0\n' > "$package/bin/sparrow-server"
chmod +x "$package/bin/sparrow-server"
(cd "$package"; sha256sum bin/sparrow-server) > "$package/SHA256SUMS"
printf '%s\n' '#!/usr/bin/env bash' 'printf "nats-server: v2.14.6\n"' > "$art/mock-nats"
chmod +x "$art/mock-nats"
printf '%s\n' '#!/usr/bin/env bash' 'set -eu' ': > "$CORE_A_MOCK_DRIVER_MARKER"' > "$art/driver"
chmod +x "$art/driver"
printf '%s\n' \
    'sparrow_runtime|core_a_tests::core_a_unit_fixture' \
    'sparrow_control|core_a_tests::core_a_broker_fixture' > "$art/expected.txt"
printf '%s\n' 'sparrow_control|core_a_tests::core_a_broker_fixture' > "$art/expected-ignored.txt"
printf '%s\n' '#!/usr/bin/env bash' 'set -eu' \
    'if [[ "$*" == *--list* ]]; then' \
    '  list="$CORE_A_MOCK_ACTUAL"' \
    '  if [[ "$*" == *--ignored* ]]; then list="$CORE_A_MOCK_IGNORED"; fi' \
    '  while IFS="|" read -r crate name; do [[ "$crate" != "$(basename "$0")" ]] || printf "%s: test\n" "$name"; done < "$list"' \
    'elif [[ "$*" != *--include-ignored* ]]; then exit 9; fi' > "$art/test-binary"
for crate in sparrow_runtime sparrow_control; do
    cp "$art/test-binary" "$frozen/reliable-test-binaries/$crate"
    chmod +x "$frozen/reliable-test-binaries/$crate"
done
jq -n '["sparrow_runtime","sparrow_control"]|map({name:.,exe:("/fixture/"+.)})' > "$frozen/reliable-test-binaries.json"
(cd "$frozen"; for crate in sparrow_runtime sparrow_control; do sha256sum "reliable-test-binaries/$crate"; done) > "$frozen/reliable-test-binaries.sha256"
for scenario in exact missing_crate missing_test renamed extra duplicate ignored_missing ignored_extra; do
    export CORE_A_MOCK_ACTUAL="$art/actual-$scenario.txt"
    export CORE_A_MOCK_IGNORED="$art/ignored-$scenario.txt"
    export CORE_A_MOCK_DRIVER_MARKER="$art/driver-$scenario.ran"
    cp "$art/expected.txt" "$CORE_A_MOCK_ACTUAL"
    cp "$art/expected-ignored.txt" "$CORE_A_MOCK_IGNORED"
    case "$scenario" in
        missing_crate) grep -v '^sparrow_control|' "$art/expected.txt" > "$CORE_A_MOCK_ACTUAL" ;;
        missing_test) sed '1d' "$art/expected.txt" > "$CORE_A_MOCK_ACTUAL" ;;
        renamed) sed '1s/core_a_/core_a_renamed_/' "$art/expected.txt" > "$CORE_A_MOCK_ACTUAL" ;;
        extra) printf '%s\n' 'sparrow_runtime|core_a_tests::core_a_extra_fixture' >> "$CORE_A_MOCK_ACTUAL" ;;
        duplicate) sed -n '1p' "$art/expected.txt" >> "$CORE_A_MOCK_ACTUAL" ;;
        ignored_missing) : > "$CORE_A_MOCK_IGNORED" ;;
        ignored_extra) cp "$art/expected.txt" "$CORE_A_MOCK_IGNORED" ;;
    esac
    result=0
    SPARROW_NATS_SERVER="$art/mock-nats" SPARROW_CORE_A_EXPECTED_TESTS="$art/expected.txt" \
        SPARROW_CORE_A_EXPECTED_IGNORED="$art/expected-ignored.txt" \
        bash "$repo/scripts/production-core-a-validate.sh" "$art/run-$scenario" "$package" "$frozen" "$art/driver" 1 \
        > "$art/$scenario.log" 2>&1 || result=$?
    if [[ "$scenario" == exact ]]; then
        test "$result" = 0; test -f "$CORE_A_MOCK_DRIVER_MARKER"
    else
        test "$result" != 0; test ! -e "$CORE_A_MOCK_DRIVER_MARKER"
    fi
    printf 'CORE_A_INVENTORY_GATE_OK %s exit=%s\n' "$scenario" "$result"
done

#!/usr/bin/env bash
# Exercise the validation gate with controlled executable fixtures, no build.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
art=${1:?new inventory-test artifact directory}
test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
expected="$repo/tests/k4-process/expected-tests.txt"
package="$art/package"; frozen="$art/frozen"
mkdir -p "$package/bin" "$frozen/reliable-test-binaries"
printf '#!/usr/bin/env bash\nexit 0\n' > "$package/bin/sparrow-server"
chmod +x "$package/bin/sparrow-server"
(cd "$package"; sha256sum bin/sparrow-server) > "$package/SHA256SUMS"
printf '%s\n' '#!/usr/bin/env bash' 'set -eu' ': > "$K4_MOCK_DRIVER_MARKER"' > "$art/driver"
chmod +x "$art/driver"
printf '%s\n' '#!/usr/bin/env bash' 'set -eu' \
  'if [[ "$*" == *--list* ]]; then' \
  '  if [[ "$*" == *--ignored* ]]; then' \
  '    if [[ "${K4_MOCK_IGNORED:-0}" == 1 && "$(basename "$0")" == sparrow_runtime ]]; then printf "iot::tests::k4_ignored_fixture: test\n"; fi' \
  '  else' \
  '    while IFS="|" read -r crate name; do [[ "$crate" != "$(basename "$0")" ]] || printf "%s: test\n" "$name"; done < "$K4_MOCK_ACTUAL"' \
  '  fi' \
  'fi' > "$art/test-binary"
for crate in sparrow_plan sparrow_runtime sparrow_control sparrow_server; do
    cp "$art/test-binary" "$frozen/reliable-test-binaries/$crate"
    chmod +x "$frozen/reliable-test-binaries/$crate"
done
jq -n '["sparrow_plan","sparrow_runtime","sparrow_control","sparrow_server"]|map({name:.,exe:("/fixture/"+.)})' > "$frozen/reliable-test-binaries.json"
(cd "$frozen"; for crate in sparrow_plan sparrow_runtime sparrow_control sparrow_server; do sha256sum "reliable-test-binaries/$crate"; done) > "$frozen/reliable-test-binaries.sha256"
grep -v '^#' "$expected" | grep -v '^$' > "$art/complete.txt"
for scenario in exact missing_crate missing_test renamed extra duplicate ignored; do
    export K4_MOCK_ACTUAL="$art/actual-$scenario.txt" K4_MOCK_IGNORED=0 K4_MOCK_DRIVER_MARKER="$art/driver-$scenario.ran"
    cp "$art/complete.txt" "$K4_MOCK_ACTUAL"
    case "$scenario" in
        missing_crate) grep -v '^sparrow_control|' "$art/complete.txt" > "$K4_MOCK_ACTUAL" ;;
        missing_test) sed '1d' "$art/complete.txt" > "$K4_MOCK_ACTUAL" ;;
        renamed) sed '1s/k4_/k4_renamed_/' "$art/complete.txt" > "$K4_MOCK_ACTUAL" ;;
        extra) printf '%s\n' 'sparrow_runtime|iot::tests::k4_extra_fixture' >> "$K4_MOCK_ACTUAL" ;;
        duplicate) sed -n '1p' "$art/complete.txt" >> "$K4_MOCK_ACTUAL" ;;
        ignored) export K4_MOCK_IGNORED=1 ;;
    esac
    result=0
    SPARROW_K4_TEST_PROFILE=reliable SPARROW_K4_EXPECTED_TESTS="$expected" \
      bash "$repo/scripts/production-k4-validate.sh" "$art/run-$scenario" "$package" "$frozen" "$art/driver" 1 > "$art/$scenario.log" 2>&1 || result=$?
    if [[ "$scenario" == exact ]]; then test "$result" == 0; test -f "$K4_MOCK_DRIVER_MARKER";
    else test "$result" != 0; test ! -e "$K4_MOCK_DRIVER_MARKER"; fi
    printf 'K4_INVENTORY_GATE_OK %s exit=%s\n' "$scenario" "$result"
done

#!/usr/bin/env bash
# Exact frozen inventory plus the real-process source-observed silence oracle
# for the reviewed v23 (File) and v24 (JetStream) profiles. The oracle SIGKILLs
# real server processes; it never stops the broker, so nothing here is a broker
# outage, a soak or a certification. No compilation happens in this script.
# Usage: ART DEFAULT_PACKAGE JS_PACKAGE FROZEN DRIVER OLD_SERVER NATS [ROUNDS]
set -euo pipefail
[[ $# -eq 7 || $# -eq 8 ]] || { printf 'usage: %s ART DEFAULT_PACKAGE JS_PACKAGE FROZEN DRIVER OLD_SERVER NATS [ROUNDS]\n' "$0" >&2; exit 2; }
root=$(cd "$(dirname "$0")/.." && pwd)
art=${1:?new evidence directory}; default=${2:?default package}; js=${3:?JetStream package}
frozen=${4:?frozen tests}; driver=${5:?silence process driver}; old=${6:?pre-v23 server}
nats=${7:?pinned broker}; rounds=${8:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
for item in "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server"; do test -x "$item"; done
command -v jq >/dev/null; command -v sha256sum >/dev/null
test -f "$frozen/reliable-test-binaries.json"; test -f "$frozen/reliable-test-binaries.sha256"
mkdir -p "$art"
art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd); default=$(cd "$default" && pwd); js=$(cd "$js" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
(cd "$default" && sha256sum -c SHA256SUMS) > "$art/default-verify.log"
(cd "$js" && sha256sum -c SHA256SUMS) > "$art/jetstream-verify.log"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"

# The reviewed manifest is the contract. It is copied into the evidence and
# every discovered name is compared against it, so a renamed, removed or newly
# added test in one of the reviewed modules cannot pass as the reviewed set.
expected=$root/tests/silence/expected-tests.txt
test -f "$expected"; test -s "$expected"
cp "$expected" "$art/expected-tests.txt"
LC_ALL=C sort "$art/expected-tests.txt" > "$art/expected.sorted.txt"
test "$(uniq -d "$art/expected.sorted.txt" | wc -l)" -eq 0
count=$(wc -l < "$art/expected.sorted.txt"); test "$count" -gt 0

# Only the reviewed silence families are discovered. A broad filter would pull
# unrelated tests into the gate, and a broad run would execute them; every name
# below is therefore invoked on its own with --exact. The complete listing is
# written to disk before any selection runs, so a short reader can never turn
# the gate into a broken-pipe failure.
declare -A binary_of seen_crate
: > "$art/discovered.txt"
i=0
while IFS=$'\t' read -r target executable; do
    crate=${target//-/_}
    # Families are matched with POSIX ERE on the materialised listing. Only the
    # reviewed silence modules are selected: a broad pattern would pull
    # unrelated tests into the gate, and a broad run would execute them.
    case $crate in
        sparrow_plan) family='^silence_tests::[^[:space:]]*: test$' ;;
        sparrow_runtime) family='^(silence_tests|observed_cut::tests)::[^[:space:]]*: test$' ;;
        sparrow_control) family='^(capability::tests::silence_|paused_time_tests::observed_time_tests::|supervisor::observed_time_log::tests::)[^[:space:]]*: test$' ;;
        *) printf '%s\n' "$crate" >> "$art/foreign-crates.txt"; continue ;;
    esac
    binary="$frozen/reliable-test-binaries/$(basename "$executable")"; test -x "$binary"
    # The complete listing lands on disk before any selection runs, so a short
    # reader can never turn the gate into a broken-pipe failure.
    "$binary" --list > "$art/list-$crate.txt"
    "$binary" --list --ignored > "$art/ignored-$crate.txt"
    grep -E "$family" "$art/list-$crate.txt" | sed 's/: test$//' > "$art/family-$crate.txt" || true
    grep -E "$family" "$art/ignored-$crate.txt" > "$art/ignored-family-$crate.txt" || true
    if test -s "$art/ignored-family-$crate.txt"; then
        printf 'ignored silence tests are not admitted: %s\n' "$crate" >&2
        exit 1
    fi
    while IFS= read -r name; do printf '%s|%s\n' "$crate" "$name" >> "$art/discovered.txt"; done \
        < "$art/family-$crate.txt"
    binary_of[$crate]=$binary; seen_crate[$crate]=1
    i=$((i+1))
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
test "$i" -gt 0
for crate in sparrow_plan sparrow_runtime sparrow_control; do
    test -n "${seen_crate[$crate]:-}" || { printf 'frozen manifest lacks %s\n' "$crate" >&2; exit 1; }
done
LC_ALL=C sort "$art/discovered.txt" > "$art/discovered.sorted.txt"
diff -u "$art/expected.sorted.txt" "$art/discovered.sorted.txt" > "$art/inventory-diff.log"

jq -n --argjson rounds "$rounds" --argjson count "$count" \
    '{rounds:$rounds,exact_tests_per_round:$count,total:($rounds*$count),
      invocation:"--exact NAME --test-threads=1",scope:"frozen deterministic repeat, not soak"}' \
    > "$art/repeat-plan.json"
# Each name runs exactly once per round with its own log, and the log must show
# one passing test: an exit code alone would also accept a filter that matched
# nothing.
mkdir -p "$art/test-logs"; : > "$art/repeat.log"
for ((round=1; round<=rounds; round++)); do
    while IFS='|' read -r crate name; do
        log="$art/test-logs/r$round-$crate-$(printf '%s' "$name" | tr -c '[:alnum:]_.' '-').log"
        printf 'ROUND=%s CRATE=%s TEST=%s LOG=%s\n' "$round" "$crate" "$name" "$log" >> "$art/repeat.log"
        if ! "${binary_of[$crate]}" --exact "$name" --test-threads=1 > "$log" 2>&1; then
            printf 'silence test failed: %s %s\n' "$crate" "$name" >&2
            tail -n 40 "$log" >&2
            exit 1
        fi
        if ! grep -q '^test result: ok\. 1 passed; 0 failed; 0 ignored' "$log"; then
            printf 'silence test did not run exactly once: %s %s\n' "$crate" "$name" >&2
            tail -n 40 "$log" >&2
            exit 1
        fi
    done < "$art/expected.sorted.txt"
done

sha256sum "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server" > "$art/binaries.sha256"
run_driver() {
    local name=$1; shift
    local rc=0
    "$driver" "$@" > "$art/$name.log" 2>&1 || rc=$?
    if [[ $rc -ne 0 ]]; then
        printf 'silence driver %s exited %s\n' "$name" "$rc" >&2
        tail -n 40 "$art/$name.log" >&2
        exit 1
    fi
    if ! grep -q '^SILENCE_PROCESS_OK$' "$art/$name.log"; then
        printf 'silence driver %s did not print its success marker\n' "$name" >&2
        tail -n 40 "$art/$name.log" >&2
        exit 1
    fi
}
run_driver default-silence --silence-file-only --server-bin "$default/bin/sparrow-server" \
    --old-server-bin "$old" --nats-server "$nats" --out "$art/default-silence"
run_driver jetstream-silence --silence-only --server-bin "$js/bin/sparrow-server" \
    --old-server-bin "$old" --nats-server "$nats" --out "$art/jetstream-silence"

# Each scenario is checked on its own before the matrix summary: a matrix that
# reports the right totals while one scenario silently disappeared, replayed
# unheld data or restarted without the committed guarantees must not pass. The
# guard map must hold exactly the reviewed transports, because `all(...)` over
# an empty or truncated map would accept a run that checked nothing.
check_scenario() { # dir transport mode version
    local dir=$1
    test -f "$dir/summary.json" || { printf 'missing silence scenario %s\n' "$dir" >&2; exit 1; }
    jq -e --arg transport "$2" --arg scenario_mode "$3" --argjson version "$4" '
        (.valid == true) and (.transport == $transport) and (.mode == $scenario_mode)
        and (.snapshot_version == $version) and (.state_kind == 12) and (.crashes == 5)
        and (.actual_sigkill == true) and (.source_health_replayed == true)
        and (.process_outage_only == true) and (.broker_outage_tested == false)
        and (.downtime_paused == true) and (.held_replay_identical == true)
        and (.committed_restart_no_repeat == true)
        and (.exactly_once_claimed == false) and (.certified == false)
    ' "$dir/summary.json" >/dev/null
    printf 'VERIFIED scenario=%s/%s version=%s\n' "$2" "$3" "$4"
}
check_package() { # label out cases crashes versions file_only guards
    local label=$1 out=$2
    check_scenario "$out/file-observed" file observed 23
    check_scenario "$out/file-registered" file registered 23
    if [[ $6 == false ]]; then
        check_scenario "$out/jetstream-observed" jetstream observed 24
        check_scenario "$out/jetstream-registered" jetstream registered 24
    fi
    test -f "$out/summary.json" || { printf 'missing silence matrix summary %s\n' "$out" >&2; exit 1; }
    jq -e --argjson cases "$3" --argjson crashes "$4" --argjson versions "$5" \
        --argjson file_only "$6" --argjson guards "$7" '
        (.valid == true) and (.transport_cases == $cases) and (.crash_scenarios == $crashes)
        and (.snapshot_versions == $versions) and (.file_only == $file_only)
        and (.actual_sigkill == true) and (.source_health_replayed == true)
        and (.process_outage_only == true) and (.broker_outage_tested == false)
        and (.exactly_once_claimed == false) and (.certified == false)
        and ((.old_profile_guards | type) == "object")
        and ((.old_profile_guards | keys) == $guards)
        and all(.old_profile_guards[];
            (.checked == true) and (.history_preserved == true)
            and (.current_preserved == true) and (.output_preserved == true))
    ' "$out/summary.json" >/dev/null
    printf 'VERIFIED matrix=%s cases=%s crashes=%s\n' "$label" "$3" "$4"
}
check_package default "$art/default-silence" 2 10 '[23]' true '["file"]'
check_package jetstream "$art/jetstream-silence" 4 20 '[23,24]' false '["file","jetstream"]'

jq -n --argjson rounds "$rounds" --argjson count "$count" \
    '{valid:true,rounds:$rounds,exact_tests_per_round:$count,total_tests:($rounds*$count),
      file_transport_cases:2,jetstream_transport_cases:4,transport_cases:6,
      file_crash_scenarios:10,jetstream_crash_scenarios:20,crash_scenarios:30,
      snapshot_versions:[23,24],actual_sigkill:true,source_health_replayed:true,
      process_outage_only:true,broker_outage_tested:false,
      source_scope:"limited source-observed silence: linear File v23 and JetStream v24 source -> silence -> required HTTP",
      source_scope_limited:true,exactly_once_claimed:false,soak:false,certified:false}' > "$art/summary.json"
printf 'SILENCE_FROZEN_AND_PROCESS_OK\n'

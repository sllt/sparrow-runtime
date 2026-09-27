#!/usr/bin/env bash
# Frozen exact tests and real SIGKILL; no builds, no performance/soak claim.
# ART DEFAULT_PACKAGE JS_PACKAGE FROZEN DRIVER OLD_SERVER NATS [ROUNDS]
set -euo pipefail
[[ $# -eq 7 || $# -eq 8 ]]
root=$(cd "$(dirname "$0")/.." && pwd)
art=$1; default=$2; js=$3; frozen=$4; driver=$5; old=$6; nats=$7; rounds=${8:-20}
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]; test ! -e "$art"
for bin in "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server"; do test -x "$bin"; done
mkdir -p "$art"; art=$(cd "$art" && pwd); frozen=$(cd "$frozen" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
(cd "$default" && sha256sum -c SHA256SUMS) > "$art/default-verify.log"
(cd "$js" && sha256sum -c SHA256SUMS) > "$art/jetstream-verify.log"
(cd "$frozen" && sha256sum -c reliable-test-binaries.sha256) > "$art/frozen-verify.log"
cp "$root/tests/resample/expected-tests.txt" "$art/expected-tests.txt"
LC_ALL=C sort "$art/expected-tests.txt" > "$art/expected.sorted"
test "$(wc -l < "$art/expected.sorted")" -eq 24
test -z "$(uniq -d "$art/expected.sorted")"
declare -A binaries
: > "$art/discovered"
while IFS=$'\t' read -r name exe; do
    case "$name" in
        sparrow_plan) family='^resample::tests::' ;;
        sparrow_runtime) family='^resample_tests::' ;;
        sparrow_control) family='^paused_time_tests::resample_tests::' ;;
        *) continue ;;
    esac
    test -z "${binaries[$name]+present}"
    binary="$frozen/reliable-test-binaries/$(basename "$exe")"; test -x "$binary"; binaries[$name]=$binary
    "$binary" --list > "$art/list-$name"
    "$binary" --list --ignored > "$art/ignored-$name"
    if grep -Eq "${family}.*: test$" "$art/ignored-$name"; then exit 1; fi
    grep -E "${family}.*: test$" "$art/list-$name" | sed -e 's/: test$//' -e "s/^/$name|/" >> "$art/discovered"
done < <(jq -r '.[]|[.name,.exe]|@tsv' "$frozen/reliable-test-binaries.json")
LC_ALL=C sort "$art/discovered" > "$art/discovered.sorted"
diff -u "$art/expected.sorted" "$art/discovered.sorted" > "$art/inventory-diff.log"
mkdir "$art/tests"
for ((round=1;round<=rounds;round++)); do
    while IFS='|' read -r crate name; do
        log="$art/tests/$round-$crate-${name//:/_}.log"
        "${binaries[$crate]}" --exact "$name" --test-threads=1 > "$log" 2>&1
        grep -q '^test result: ok\. 1 passed; 0 failed; 0 ignored;' "$log"
    done < "$art/expected.sorted"
done
sha256sum "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server" > "$art/binaries.sha256"
for variant in default jetstream; do
    package=$default; mode=--resample-file-only; count=3; crashes=10; file_only=true; versions='[25]'; guards=1
    if [[ "$variant" == jetstream ]]; then package=$js; mode=--resample-only; count=6; crashes=20; file_only=false; versions='[25,26]'; guards=2; fi
    "$driver" "$mode" --server-bin "$package/bin/sparrow-server" --old-server-bin "$old" --nats-server "$nats" --out "$art/$variant" > "$art/$variant.log" 2>&1
    grep -qx RESAMPLE_PROCESS_OK "$art/$variant.log"
    jq -e --argjson count "$count" --argjson crashes "$crashes" --argjson file_only "$file_only" --argjson versions "$versions" --argjson guards "$guards" '
        .valid == true and .transport_cases == $count and .crash_scenarios == $crashes
        and .file_only == $file_only and .snapshot_versions == $versions and .actual_sigkill == true
        and .modes == ["last","mean","interpolate"] and .exactly_once_claimed == false and .certified == false
        and (.old_profile_guards|length) == $guards
        and all(.old_profile_guards[]; .checked == true and .history_preserved == true and .current_preserved == true and .output_preserved == true)' "$art/$variant/summary.json" >/dev/null
    transports=(file); if [[ "$variant" == jetstream ]]; then transports+=(jetstream); fi
    for transport in "${transports[@]}"; do for sampling in last mean interpolate; do
        n=3; waiting=false; if [[ "$sampling" == interpolate ]]; then n=4; waiting=true; fi
        version=25; broker=false; if [[ "$transport" == jetstream ]]; then version=26; broker=true; fi
        jq -e --arg mode "$sampling" --argjson version "$version" --argjson broker "$broker" --argjson n "$n" --argjson waiting "$waiting" '
            .valid == true and .mode == $mode and .snapshot_version == $version and .jetstream == $broker
            and .actual_sigkill == true and .crash_scenarios == $n and .pending_replay_identical == true
            and .committed_restart_no_repeat == true and .downtime_paused == true
            and .interpolation_wait_restore_checked == $waiting and .certified == false' "$art/$variant/$transport-$sampling/summary.json" >/dev/null
    done; done
done
jq -n --argjson rounds "$rounds" '{valid:true,rounds:$rounds,tests_per_round:24,
    process_scenarios:9,crash_scenarios:30,snapshot_versions:[25,26],actual_sigkill:true,
    performance:false,soak:false,certified:false}' > "$art/summary.json"
printf 'RESAMPLE_VALIDATION_OK\n'

#!/usr/bin/env bash
# Used exclusively by runner-contract.sh; every process claim below is a stub.
set -euo pipefail
mode=${RESAMPLE_STUB_MODE:-ok}
case "$(basename "$0")" in
    sparrow_plan|sparrow_runtime|sparrow_control)
        crate=$(basename "$0")
        if [[ " $* " == *' --list '* ]]; then
            if [[ " $* " != *' --ignored '* ]]; then
                grep "^$crate|" "$RESAMPLE_EXPECTED" | cut -d'|' -f2 | sed 's/$/: test/'
            fi
        else
            count=1; [[ "$mode" != zero-tests ]] || count=0
            printf 'test result: ok. %s passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n' "$count"
        fi
        exit 0 ;;
esac
[[ "$mode" != driver-fail ]] || exit 7
file_only=false; out=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --resample-file-only) file_only=true; shift ;;
        --resample-only) shift ;;
        --out) out=$2; shift 2 ;;
        --server-bin|--old-server-bin|--nats-server) shift 2 ;;
        *) exit 2 ;;
    esac
done
test -n "$out"; test ! -e "$out"; mkdir -p "$out"
transports=(file); versions='[25]'; count=3; crashes=10
if [[ "$file_only" == false ]]; then transports+=(jetstream); versions='[25,26]'; count=6; crashes=20; fi
guards='{}'
for transport in "${transports[@]}"; do
    guards=$(jq -nc --argjson old "$guards" --arg key "$transport" '$old+{($key):{checked:true,history_preserved:true,current_preserved:true,output_preserved:true}}')
    version=25; js=false; if [[ "$transport" == jetstream ]]; then version=26; js=true; fi
    for sampling in last mean interpolate; do
        mkdir "$out/$transport-$sampling"
        n=3; waiting=false; if [[ "$sampling" == interpolate ]]; then n=4; waiting=true; fi
        jq -n --arg mode "$sampling" --argjson js "$js" --argjson version "$version" --argjson n "$n" --argjson waiting "$waiting" \
            '{valid:true,mode:$mode,jetstream:$js,snapshot_version:$version,actual_sigkill:true,crash_scenarios:$n,
            pending_replay_identical:true,committed_restart_no_repeat:true,downtime_paused:true,
            interpolation_wait_restore_checked:$waiting,certified:false}' > "$out/$transport-$sampling/summary.json"
    done
done
[[ "$mode" != empty-guards ]] || guards='{}'
[[ "$mode" != wrong-version ]] || versions='[24]'
[[ "$mode" != wrong-crashes ]] || crashes=1
jq -n --argjson f "$file_only" --argjson versions "$versions" --argjson count "$count" --argjson crashes "$crashes" --argjson guards "$guards" \
    '{valid:true,file_only:$f,snapshot_versions:$versions,transport_cases:$count,crash_scenarios:$crashes,
    actual_sigkill:true,modes:["last","mean","interpolate"],old_profile_guards:$guards,exactly_once_claimed:false,certified:false}' > "$out/summary.json"
case "$mode" in
    missing-replay|missing-wait)
        key=pending_replay_identical; [[ "$mode" != missing-wait ]] || key=interpolation_wait_restore_checked
        target="$out/file-interpolate/summary.json"
        jq --arg key "$key" 'del(.[$key])' "$target" > "$target.tmp"; mv "$target.tmp" "$target" ;;
    missing-case) rm "$out/file-mean/summary.json" ;;
esac
printf 'RESAMPLE_PROCESS_OK\n'

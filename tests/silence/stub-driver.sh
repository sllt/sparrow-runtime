#!/usr/bin/env bash
# Stub stand-in for the Go silence process driver, used only by
# tests/silence/runner-contract.sh. It writes JSON in the reviewed shape and
# never starts a server, so it cannot claim a real SIGKILL, a broker outage or
# any observed-time coverage.
# Failure injection: SPARROW_STUB_MODE, SPARROW_STUB_FAIL_PACKAGE.
set -euo pipefail
mode=${SPARROW_STUB_MODE:-ok}
file_only=0; out=; server=
while [[ $# -gt 0 ]]; do
    case $1 in
        --silence-file-only) file_only=1; shift ;;
        --silence-only) file_only=0; shift ;;
        --server-bin) server=$2; shift 2 ;;
        --old-server-bin|--nats-server) shift 2 ;;
        --out) out=$2; shift 2 ;;
        *) printf 'stub-driver: unexpected argument %s\n' "$1" >&2; exit 64 ;;
    esac
done
[[ -n $out && -n $server ]] || { printf 'stub-driver: --out and --server-bin are required\n' >&2; exit 64; }
if [[ -n ${SPARROW_STUB_FAIL_PACKAGE:-} && $server == *"$SPARROW_STUB_FAIL_PACKAGE"* ]]; then
    mode=driver-fail
fi
[[ $mode != driver-fail ]] || { printf 'stub-driver: injected driver failure\n' >&2; exit 3; }
mkdir -p "$out"

# One summary per scenario directory, exactly as the reviewed matrix lays them
# out: <transport>-<mode>.
scenario() { # transport mode version
    local transport=$1 scenario_mode=$2 version=$3 crashes=5
    local sigkill=true downtime=true replay=true committed=true kind=12
    case $mode in
        missing-crash) crashes=4 ;;
        false-sigkill) sigkill=false ;;
        false-replay) replay=false ;;
        false-commit) committed=false ;;
        false-downtime) downtime=false ;;
        wrong-kind) kind=11 ;;
        wrong-version) version=$((version - 1)) ;;
    esac
    local dir="$out/$transport-$scenario_mode"
    if [[ $mode == missing-subscenario && $transport-$scenario_mode == jetstream-registered ]]; then
        return 0
    fi
    mkdir -p "$dir"
    jq -n --arg transport "$transport" --arg scenario_mode "$scenario_mode" \
        --argjson version "$version" --argjson crashes "$crashes" --argjson kind "$kind" \
        --argjson sigkill "$sigkill" --argjson downtime "$downtime" \
        --argjson replay "$replay" --argjson committed "$committed" '
        {valid:true,transport:$transport,mode:$scenario_mode,snapshot_version:$version,
         state_kind:$kind,crashes:$crashes,actual_sigkill:$sigkill,
         source_health_replayed:true,process_outage_only:true,broker_outage_tested:false,
         downtime_paused:$downtime,held_replay_identical:$replay,
         committed_restart_no_repeat:$committed,exactly_once_claimed:false,certified:false}
    ' > "$dir/summary.json"
}
scenario file observed 23
scenario file registered 23
if (( file_only == 0 )); then
    scenario jetstream observed 24
    scenario jetstream registered 24
fi

cases=4; crashes=20; versions='[23,24]'
if (( file_only )); then
    cases=2; crashes=10; versions='[23]'
fi
guard_map='{"file":{"checked":true,"history_preserved":true,"current_preserved":true,"output_preserved":true},
    "jetstream":{"checked":true,"history_preserved":true,"current_preserved":true,"output_preserved":true}}'
if (( file_only )); then
    guard_map='{"file":{"checked":true,"history_preserved":true,"current_preserved":true,"output_preserved":true}}'
fi
case $mode in
    missing-crash) crashes=$((crashes - 1)) ;;
    wrong-version) versions='[23]' ;;
    guard) if (( file_only == 0 )); then
               guard_map='{"file":{"checked":true,"history_preserved":true,"current_preserved":true,"output_preserved":true}}'
           fi ;;
    empty-guards) guard_map='{}' ;;
esac
sigkill=true; [[ $mode != false-sigkill ]] || sigkill=false
broker=false; [[ $mode != broker-outage-claim ]] || broker=true
jq -n --argjson cases "$cases" --argjson crashes "$crashes" --argjson versions "$versions" \
    --argjson file_only "$((file_only == 1))" --argjson sigkill "$sigkill" --argjson broker "$broker" \
    --argjson guards "$guard_map" '
    {valid:true,transport_cases:$cases,crash_scenarios:$crashes,snapshot_versions:$versions,
     file_only:($file_only==1),
     source_scope:"linear File append_only / JetStream -> silence -> required HTTP",
     actual_sigkill:$sigkill,source_health_replayed:true,
     process_outage_only:true,broker_outage_tested:$broker,
     old_profile_guards:$guards,exactly_once_claimed:false,certified:false}
' > "$out/summary.json"
[[ $mode != missing-marker ]] || exit 0
printf 'SILENCE_PROCESS_OK\n'

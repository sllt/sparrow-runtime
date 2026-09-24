#!/usr/bin/env bash
# Stub stand-in for the Go alarm-graph process driver, used only by
# tests/alarm-graph/runner-contract.sh. It writes JSON of the reviewed shape and
# never starts a server, so it cannot claim a real SIGKILL or graph result.
# Failure injection: SPARROW_STUB_MODE, SPARROW_STUB_FAIL_PACKAGE.
set -euo pipefail
mode=${SPARROW_STUB_MODE:-ok}
out=; server=
while [[ $# -gt 0 ]]; do
    case $1 in
        --alarm-graph-only) shift ;;
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
for kind in branch-union partial-sinks; do
    mkdir -p "$out/$kind"
    jq -n --arg kind "$kind" '
        {valid:true, kind:$kind, real_sigkill:true, pending_replay_identical:true,
         committed_restart_no_repeat:true, downtime_paused:true,
         activate_resolve_episode_preserved:true}
        + (if $kind == "branch-union" then {operator_namespaces_distinct:true}
           else {partial_sink_flush_checked:true} end)
    ' > "$out/$kind/summary.json"
done
case $mode in
    missing-sigkill)
        jq 'del(.real_sigkill)' "$out/branch-union/summary.json" > "$out/stub.tmp"
        mv "$out/stub.tmp" "$out/branch-union/summary.json" ;;
    missing-episode)
        jq 'del(.activate_resolve_episode_preserved)' "$out/partial-sinks/summary.json" > "$out/stub.tmp"
        mv "$out/stub.tmp" "$out/partial-sinks/summary.json" ;;
esac
version=22
[[ $mode != bad-version ]] || version=21
guards='{"branch-union":{"checked":true,"history_preserved":true,"current_preserved":true,"output_preserved":true},
    "partial-sinks":{"checked":true,"history_preserved":true,"current_preserved":true,"output_preserved":true}}'
[[ $mode != empty-guards ]] || guards='{}'
jq -n --argjson guards "$guards" --argjson version "$version" \
    '{valid:true,process_scenarios:2,crash_scenarios:2,snapshot_versions:[$version],
      old_profile_guards:$guards,exactly_once_claimed:false,certified:false}' > "$out/summary.json"
printf 'STUB_ALARM_GRAPH_OUT out=%s server=%s mode=%s\n' "$out" "$server" "$mode"

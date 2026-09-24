#!/usr/bin/env bash
# v22 File alarm-graph SIGKILL acceptance for the packaged default and JetStream
# binaries. The JetStream package is exercised as a binary; this is not a
# JetStream-source graph claim. No compilation, no soak, no certification.
# Usage: bash scripts/production-alarm-graph-validate.sh ART DEFAULT_PACKAGE JS_PACKAGE DRIVER OLD_SERVER NATS
set -euo pipefail
[[ $# -eq 6 ]] || { printf 'usage: %s ART DEFAULT_PACKAGE JS_PACKAGE DRIVER OLD_SERVER NATS\n' "$0" >&2; exit 2; }
art=${1:?new evidence directory}; default=${2:?default package}; js=${3:?JetStream package}
driver=${4:?alarm-graph process driver}; old=${5:?pre-v22 server}; nats=${6:?pinned broker}
test ! -e "$art"
for item in "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server"; do test -x "$item"; done
command -v jq >/dev/null
mkdir -p "$art"
art=$(cd "$art" && pwd); default=$(cd "$default" && pwd); js=$(cd "$js" && pwd)
trap 'printf "%s\n" "$?" > "$art/exit"' EXIT
(cd "$default" && sha256sum -c SHA256SUMS) > "$art/default-verify.log"
(cd "$js" && sha256sum -c SHA256SUMS) > "$art/jetstream-verify.log"
sha256sum "$driver" "$old" "$nats" "$default/bin/sparrow-server" "$js/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --alarm-graph-only --server-bin "$default/bin/sparrow-server" --old-server-bin "$old" \
    --nats-server "$nats" --out "$art/default-graph" > "$art/default-graph.log" 2>&1
"$driver" --alarm-graph-only --server-bin "$js/bin/sparrow-server" --old-server-bin "$old" \
    --nats-server "$nats" --out "$art/jetstream-graph" > "$art/jetstream-graph.log" 2>&1

# Matrix summary plus both scenario summaries, checked independently per package.
# The guard map must hold exactly the two reviewed keys, otherwise `all(...)`
# over an empty object would pass vacuously.
check_package() {
    local label=$1 out=$2 kind
    jq -e '
        (.valid == true) and (.process_scenarios == 2) and (.crash_scenarios == 2)
        and (.snapshot_versions == [22])
        and (.exactly_once_claimed == false) and (.certified == false)
        and ((.old_profile_guards | type) == "object")
        and ((.old_profile_guards | keys) == ["branch-union", "partial-sinks"])
        and all(.old_profile_guards[];
            (.checked == true) and (.history_preserved == true)
            and (.current_preserved == true) and (.output_preserved == true))
    ' "$out/summary.json" >/dev/null
    for kind in branch-union partial-sinks; do
        jq -e --arg kind "$kind" '
            (.valid == true) and (.kind == $kind)
            and (.real_sigkill == true) and (.pending_replay_identical == true)
            and (.committed_restart_no_repeat == true) and (.downtime_paused == true)
            and (.activate_resolve_episode_preserved == true)
            and ((.partial_sink_flush_checked // false) == ($kind == "partial-sinks"))
            and ((.operator_namespaces_distinct // false) == ($kind == "branch-union"))
        ' "$out/$kind/summary.json" >/dev/null
        printf 'VERIFIED package=%s scenario=%s\n' "$label" "$kind"
    done
}
check_package default "$art/default-graph"
check_package jetstream "$art/jetstream-graph"
jq -n '{valid:true,process_scenarios:4,crash_scenarios:4,per_package_scenarios:2,snapshot_versions:[22],
    graph_process_sigkill:true,source_scope:"file",exactly_once_claimed:false,soak:false,certified:false}' \
    > "$art/summary.json"
printf 'ALARM_GRAPH_PROCESS_OK\n'

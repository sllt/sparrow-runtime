#!/usr/bin/env bash
# Isolated processes and package directories only. No existing service changes.
set -euo pipefail
umask 077
unset SPARROW_SAFE_MODE
server=$(realpath "${1:?server binary}")
ctl=$(realpath "${2:?sparrowctl binary}")
root=${3:?new evidence directory}
repo=$(cd "$(dirname "$0")/.." && pwd)
for tool in ss jq sha256sum timeout cc; do command -v "$tool" >/dev/null; done
test ! -e "$root"; mkdir -p "$root"; root=$(cd "$root" && pwd)
port=${SPARROW_PLUGIN_TEST_PORT:-$((21000+$$%6000))}
test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_TOKEN=plugin-isolated-smoke-not-a-deployment-secret
export SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef
export SPARROW_REQUIRE_SECRETS_KEY=1 SPARROW_DATA_ROOTS="$root"
export SPARROW_URL="http://127.0.0.1:$port"
export SPARROW_PLUGIN_DIR="$root/plugins" SPARROW_ENABLE_NATIVE_PLUGINS=1
pid=
etag=
stop(){ if [[ -n "$pid" ]]; then kill -TERM "$pid"; wait "$pid"; pid=; fi; }
cleanup(){ if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; }
trap cleanup EXIT
start(){
  "$server" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" "$@" >> "$root/server.log" 2>&1 & pid=$!
  for _ in $(seq 1 100); do
    if "$ctl" health > "$root/health.json" 2>/dev/null; then return; fi
    kill -0 "$pid"; sleep .1
  done
  printf 'server startup timed out\n' >&2; return 1
}
reject(){ local output=$1; shift; if "$@" > "$root/$output" 2>&1; then printf 'unexpected acceptance: %s\n' "$output" >&2; exit 1; else test "$?" = 2; fi; }
wait_state(){
  for _ in $(seq 1 100); do
    "$ctl" status native > "$root/status.json"
    if jq -e --arg s "$1" '.actual.status==$s' "$root/status.json" >/dev/null; then return; fi
    sleep .05
  done
  printf 'pipeline state timeout\n' >&2; return 1
}
run_version(){
  local version=$1 digest=$2 expected=$3 directory=$4
  mkdir "$root/$directory"
  jq -n --arg input "$root/input.ndjson" --arg output "$root/$directory" \
    --arg sql "SELECT plugin_call('native_math','$version','$digest','double',value) AS doubled FROM s" \
    '{version:1,stream:"s",sql:$sql,recovery:"restart_fresh",fail_on_decode:true,source:{kind:"file",path:$input,file_contract:"append_only"},sink:{kind:"file",file:{directory:$output,segment_bytes:1048576,max_bytes:2097152,max_files:4,row_bytes:65536,sync_data:true}}}' > "$root/pipeline.json"
  local options=()
  if [[ -n "$etag" ]]; then options=(--if-match "$etag"); fi
  "$ctl" put-pipeline native "$root/pipeline.json" "${options[@]}" > "$root/put-$directory.json"
  etag=$(jq -er '.etag' "$root/put-$directory.json")
  "$ctl" start native > "$root/start-$directory.json"; wait_state running
  local ok=0
  for _ in $(seq 1 100); do
    if compgen -G "$root/$directory/*.ndjson" >/dev/null && jq -s -e --argjson n "$expected" 'length==1 and .[0].doubled==$n' "$root/$directory/"*.ndjson >/dev/null; then ok=1; break; fi
    sleep .05
  done
  test "$ok" = 1
  reject "pinned-$directory.json" "$ctl" plugin-disable "$digest"
  "$ctl" stop native > "$root/stop-$directory.json"; wait_state stopped
}
bash "$repo/scripts/build-native-plugin-example.sh" "$root/v1"
mkdir "$root/v2"
cc -std=c11 -O2 -fPIC -shared -Wall -Wextra -Werror -DSPARROW_PLUGIN_FACTOR=3 -I"$repo/sdk/native" "$repo/examples/plugins/native_math.c" -o "$root/v2/native_math.so"
hash=$(sha256sum "$root/v2/native_math.so");hash=${hash%% *}
jq --arg hash "$hash" '.version="v2" | .artifact_sha256=$hash' "$root/v1/manifest.json" > "$root/v2/manifest.json"
printf '{"value":21}\n' > "$root/input.ndjson"
printf '{"fields":[{"name":"value","type":"int64","nullable":false}]}\n' > "$root/schema.json"
start
"$ctl" put-stream s "$root/schema.json" > "$root/stream.json"
"$ctl" plugin-install "$root/v1/manifest.json" "$root/v1/native_math.so" > "$root/install-v1.json"
old=$(jq -er '.manifest_sha256' "$root/install-v1.json")
"$ctl" plugin-enable "$old" > "$root/enable-v1.json"
run_version v1 "$old" 42 first
"$ctl" plugin-disable "$old" > "$root/disable-v1.json"
"$ctl" plugin-install "$root/v2/manifest.json" "$root/v2/native_math.so" > "$root/install-v2.json"
new=$(jq -er '.manifest_sha256' "$root/install-v2.json")
"$ctl" plugin-enable "$new" > "$root/enable-v2.json"
run_version v2 "$new" 63 upgrade
"$ctl" plugin-enable "$old" > "$root/reenable-v1.json"
run_version v1 "$old" 42 rollback
stop; start --safe-mode
"$ctl" plugins > "$root/safe-mode.json"
jq -e '.packages|length==2 and all(.[]; .desired_enabled and (.enabled|not))' "$root/safe-mode.json" >/dev/null
reject safe-mode-enable.json "$ctl" plugin-enable "$old"
stop; start
"$ctl" plugins > "$root/restarted.json"
jq -e '.packages|length==2 and all(.[]; .enabled)' "$root/restarted.json" >/dev/null
for id in "$old" "$new"; do "$ctl" plugin-disable "$id" > "$root/disable-$id.json"; reject "resident-$id.json" "$ctl" plugin-uninstall "$id"; done
stop; start
for id in "$old" "$new"; do "$ctl" plugin-uninstall "$id" > "$root/uninstall-$id.json"; done
"$ctl" plugins > "$root/empty.json"; jq -e '.packages|length==0' "$root/empty.json" >/dev/null
reject missing-dependency.json "$ctl" validate "$root/pipeline.json"
stop
printf '{"passed":true,"upgrade_outputs":[42,63,42],"safe_mode":true,"restart":true,"resident_uninstall_rejected":true,"missing_dependency_rejected":true}\n' > "$root/result.json"
printf 'native plugin process smoke passed\n'

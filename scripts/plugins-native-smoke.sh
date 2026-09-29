#!/usr/bin/env bash
# Isolated processes and package directories only. No existing service changes.
set -euo pipefail
umask 077
unset SPARROW_SAFE_MODE SPARROW_PLUGIN_TRUST_STORE
server=$(realpath "${1:?server binary}")
ctl=$(realpath "${2:?sparrowctl binary}")
root=${3:?new evidence directory}
repo=$(cd "$(dirname "$0")/.." && pwd)
kind=${SPARROW_PLUGIN_SMOKE_KIND:-native}
case "$kind" in native|script|wasm) ;; *) exit 2;; esac
package=${kind}_math
artifact=native_math.so
for tool in ss jq sha256sum timeout; do command -v "$tool" >/dev/null; done
test ! -e "$root"; mkdir -p "$root"; root=$(cd "$root" && pwd)
port=${SPARROW_PLUGIN_TEST_PORT:-$((21000+$$%6000))}
test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_TOKEN=plugin-isolated-smoke-not-a-deployment-secret
export SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef
export SPARROW_REQUIRE_SECRETS_KEY=1 SPARROW_DATA_ROOTS="$root"
export SPARROW_URL="http://127.0.0.1:$port"
export SPARROW_PLUGIN_DIR="$root/plugins" SPARROW_ENABLE_NATIVE_PLUGINS=1
export SPARROW_ENABLE_SCRIPT_PLUGINS=0 SPARROW_ENABLE_WASM_PLUGINS=0
if [[ "$kind" == script ]]; then
  export SPARROW_ENABLE_NATIVE_PLUGINS=0 SPARROW_ENABLE_SCRIPT_PLUGINS=1
  artifact=script_math.js
fi
if [[ "$kind" == wasm ]]; then
  export SPARROW_ENABLE_NATIVE_PLUGINS=0 SPARROW_ENABLE_WASM_PLUGINS=1
  artifact=wasm_math.wasm
fi
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
    --arg sql "SELECT plugin_call('$package','$version','$digest','double',value) AS doubled FROM s" \
    '{version:1,stream:"s",sql:$sql,recovery:"restart_fresh",fail_on_decode:true,source:{kind:"file",path:$input,file_contract:"append_only"},sink:{kind:"file",file:{directory:$output,segment_bytes:1048576,max_bytes:2097152,max_files:4,row_bytes:65536,sync_data:true}}}' > "$root/pipeline.json"
  local options=()
  if [[ -n "$etag" ]]; then options=(--if-match "$etag"); fi
  "$ctl" put-pipeline native "$root/pipeline.json" "${options[@]}" > "$root/put-$directory.json"
  etag=$(jq -er '.etag' "$root/put-$directory.json")
  "$ctl" start native > "$root/start-$directory.json"
  if [[ "$expected" == fail ]]; then
    wait_state failed
    "$ctl" health > "$root/health-after-failure.json"
    "$ctl" stop native > "$root/stop-$directory.json"; wait_state stopped
    if compgen -G "$root/$directory/*.ndjson" >/dev/null; then
      jq -s -e 'length==0' "$root/$directory/"*.ndjson >/dev/null
    fi
    return
  fi
  wait_state running
  local ok=0
  for _ in $(seq 1 100); do
    if compgen -G "$root/$directory/*.ndjson" >/dev/null && jq -s -e --argjson n "$expected" 'length==1 and .[0].doubled==$n' "$root/$directory/"*.ndjson >/dev/null; then ok=1; break; fi
    sleep .05
  done
  test "$ok" = 1
  reject "pinned-$directory.json" "$ctl" plugin-disable "$digest"
  "$ctl" stop native > "$root/stop-$directory.json"; wait_state stopped
}
bash "$repo/scripts/build-$kind-plugin-example.sh" "$root/v1"
mkdir "$root/v2"
if [[ "$kind" == native ]]; then
  cc -std=c11 -O2 -fPIC -shared -Wall -Wextra -Werror -DSPARROW_PLUGIN_FACTOR=3 -I"$repo/sdk/native" "$repo/examples/plugins/native_math.c" -o "$root/v2/$artifact"
elif [[ "$kind" == wasm ]]; then
  sed -e 's/(i64.const 2)))/(i64.const 3)))/' -e 's/4611686018427387903/3074457345618258602/' -e 's/-4611686018427387904/-3074457345618258602/' "$repo/examples/plugins/wasm_math.wat" > "$root/v2/wasm_math.wat"
  "$SPARROW_WASM_PACK" "$root/v2/wasm_math.wat" "$root/v2/$artifact"
else
  sed 's/value \* 2n/value * 3n/' "$root/v1/$artifact" > "$root/v2/$artifact"
fi
hash=$(sha256sum "$root/v2/$artifact");hash=${hash%% *}
jq --arg hash "$hash" '.version="v2" | .artifact_sha256=$hash' "$root/v1/manifest.json" > "$root/v2/manifest.json"
printf '{"value":21}\n' > "$root/input.ndjson"
printf '{"fields":[{"name":"value","type":"int64","nullable":false}]}\n' > "$root/schema.json"
start
"$ctl" put-stream s "$root/schema.json" > "$root/stream.json"
"$ctl" plugin-install "$root/v1/manifest.json" "$root/v1/$artifact" > "$root/install-v1.json"
old=$(jq -er '.manifest_sha256' "$root/install-v1.json")
"$ctl" plugin-enable "$old" > "$root/enable-v1.json"
jq -n --arg sql "SELECT plugin_call('$package','v1','$old','double',value) AS doubled FROM s" \
  '{sql:$sql,inputs:[{stream:"s",rows:[{value:21}]}]}' > "$root/query.json"
if [[ "$kind" != native ]]; then
  "$ctl" query "$root/query.json" > "$root/query-success.json"
  jq -e '.complete and .rows==[{doubled:42}]' "$root/query-success.json" >/dev/null
  jq '.limits={work_units:10000}' "$root/query.json" > "$root/query-budget.json"
  reject query-budget-rejected.json "$ctl" query "$root/query-budget.json"
  grep -q 'aggregate .* work limit' "$root/query-budget-rejected.json"
  "$ctl" plugins > "$root/cache.json"
  if [[ "$kind" == script ]]; then
    jq -e '.packages|all(.[]; .script_cache.compiled_scripts==2 and .script_cache.bytes>0 and .script_cache.bytes<=262144 and .script_cache.artifact_sha256==.manifest.artifact_sha256)' "$root/cache.json" >/dev/null
  else
    jq -e '.packages|all(.[]; .wasm_module.compiled_scripts==1 and .wasm_module.bytes>0 and .wasm_module.bytes<=131072 and .wasm_module.artifact_sha256==.manifest.artifact_sha256)' "$root/cache.json" >/dev/null
  fi
else
  reject query-native-rejected.json "$ctl" query "$root/query.json"
fi
run_version v1 "$old" 42 first
"$ctl" plugin-disable "$old" > "$root/disable-v1.json"
"$ctl" plugin-install "$root/v2/manifest.json" "$root/v2/$artifact" > "$root/install-v2.json"
new=$(jq -er '.manifest_sha256' "$root/install-v2.json")
"$ctl" plugin-enable "$new" > "$root/enable-v2.json"
run_version v2 "$new" 63 upgrade
"$ctl" plugin-enable "$old" > "$root/reenable-v1.json"
run_version v1 "$old" 42 rollback
if [[ "$kind" == script ]]; then
  mkdir "$root/diagnostic"
  printf '%s\n' "({double(v){throw new Error('PRIVATE_PAYLOAD');}})" > "$root/diagnostic/script_math.js"
  hash=$(sha256sum "$root/diagnostic/script_math.js"); hash=${hash%% *}
  jq --arg hash "$hash" '.version="diagnostic" | .artifact_sha256=$hash | .functions=[.functions[0]]' "$root/v1/manifest.json" > "$root/diagnostic/manifest.json"
  "$ctl" plugin-install "$root/diagnostic/manifest.json" "$root/diagnostic/script_math.js" > "$root/install-diagnostic.json"
  diagnostic=$(jq -er '.manifest_sha256' "$root/install-diagnostic.json")
  "$ctl" plugin-enable "$diagnostic" > "$root/enable-diagnostic.json"
  jq --arg sql "SELECT plugin_call('$package','diagnostic','$diagnostic','double',value) AS doubled FROM s" '.sql=$sql' "$root/query.json" > "$root/query-diagnostic.json"
  reject query-diagnostic-rejected.json "$ctl" query "$root/query-diagnostic.json"
  jq -e '.error.context|any(.[]; .key=="script_phase" and .value=="call")' "$root/query-diagnostic-rejected.json" >/dev/null
  jq -e '.error.context|any(.[]; .key=="script_frames" and (.value|test("^1:[0-9]+")))' "$root/query-diagnostic-rejected.json" >/dev/null
  ! grep -q PRIVATE_PAYLOAD "$root/query-diagnostic-rejected.json"
  "$ctl" plugin-disable "$diagnostic" > "$root/disable-diagnostic.json"
  "$ctl" plugin-uninstall "$diagnostic" > "$root/uninstall-diagnostic.json"
  mkdir "$root/bad"
  printf '%s\n' "({double(value) { /(a+)+\$/.test('a'.repeat(30)+'!'); return value; }})" > "$root/bad/script_math.js"
  hash=$(sha256sum "$root/bad/script_math.js"); hash=${hash%% *}
  jq --arg hash "$hash" '.version="bad" | .artifact_sha256=$hash | .functions=[.functions[0]]' "$root/v1/manifest.json" > "$root/bad/manifest.json"
  "$ctl" plugin-install "$root/bad/manifest.json" "$root/bad/script_math.js" > "$root/install-bad.json"
  bad=$(jq -er '.manifest_sha256' "$root/install-bad.json")
  "$ctl" plugin-enable "$bad" > "$root/enable-bad.json"
  run_version bad "$bad" fail failure
  "$ctl" plugins > "$root/after-failure.json"
  jq -e --arg hash "$bad" '.packages[]|select(.manifest_sha256==$hash)|.script_worker_state=="failed"' "$root/after-failure.json" >/dev/null
  reject failed-worker-enable.json "$ctl" plugin-enable "$bad"
  "$ctl" plugin-disable "$bad" > "$root/disable-bad.json"
  reject retained-bad.json "$ctl" plugin-uninstall "$bad"
  "$ctl" retire-pipeline native "$etag" > "$root/retire-bad-history.json"
  etag=
  "$ctl" plugin-uninstall "$bad" > "$root/uninstall-bad.json"
  run_version v1 "$old" 42 after-failure
fi
stop; start --safe-mode
"$ctl" plugins > "$root/safe-mode.json"
jq -e '.packages|length==2 and all(.[]; .desired_enabled and (.enabled|not))' "$root/safe-mode.json" >/dev/null
reject safe-mode-enable.json "$ctl" plugin-enable "$old"
stop; start
"$ctl" plugins > "$root/restarted.json"
jq -e '.packages|length==2 and all(.[]; .enabled)' "$root/restarted.json" >/dev/null
"$ctl" plugin-references "$old" > "$root/references.json"
jq -e '.count>0' "$root/references.json" >/dev/null
reject retained-history.json "$ctl" plugin-uninstall "$old"
"$ctl" retire-pipeline native "$etag" > "$root/retire-history.json"
for id in "$old" "$new"; do
  "$ctl" plugin-disable "$id" > "$root/disable-$id.json"
  if [[ "$kind" == native ]]; then reject "resident-$id.json" "$ctl" plugin-uninstall "$id"; fi
done
# JavaScript needs no restart to uninstall; native deliberately still does.
if [[ "$kind" == native ]]; then
stop; start
fi
for id in "$old" "$new"; do "$ctl" plugin-uninstall "$id" > "$root/uninstall-$id.json"; done
"$ctl" plugins > "$root/empty.json"; jq -e '.packages|length==0' "$root/empty.json" >/dev/null
reject missing-dependency.json "$ctl" validate "$root/pipeline.json"
stop
jq -n --arg kind "$kind" '{passed:true,kind:$kind,upgrade_outputs:[42,63,42],safe_mode:true,restart:true,resident_uninstall_rejected:($kind=="native"),hot_unload:($kind!="native"),script_failure_isolated:($kind=="script"),missing_dependency_rejected:true}' > "$root/result.json"
printf '%s plugin process smoke passed\n' "$kind"

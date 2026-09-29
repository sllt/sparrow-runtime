#!/usr/bin/env bash
# Isolated real Server/CLI acceptance; never touches an existing deployment.
set -euo pipefail
umask 077
unset SPARROW_SAFE_MODE
server=$(realpath "${1:?server}");ctl=$(realpath "${2:?ctl}");sign=$(realpath "${3:?sign}")
sample=$(realpath "${4:?prebuilt extension example directory}");root=${5:?new evidence directory}
test ! -e "$root";mkdir -p "$root";root=$(cd "$root"&&pwd)
port=$((28000+$$%3000));test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_TOKEN=external-test-not-a-deployment-secret SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef
export SPARROW_PLUGIN_DIR="$root/plugins" SPARROW_PLUGIN_TRUST_STORE="$root/trust.json" SPARROW_DATA_ROOTS="$root" SPARROW_URL="http://127.0.0.1:$port"
export SPARROW_ENABLE_NATIVE_PLUGINS=0 SPARROW_ENABLE_SCRIPT_PLUGINS=0 SPARROW_ENABLE_WASM_PLUGINS=0 SPARROW_ENABLE_EXTERNAL_PLUGINS=0
"$sign" keygen "$root/private.pk8" "$root/public.json" > "$root/keygen.json"
jq '{format:1,require_signed:true,publishers:[{id:"publisher",public_key_base64:.public_key_base64,packages:["*"],kinds:["native_extension"],revoked:false}]}' "$root/public.json" > "$root/trust.json"
pid=;etag=
cleanup(){ if [[ -n "$pid" ]];then kill "$pid" 2>/dev/null||true;wait "$pid" 2>/dev/null||true;fi; }
trap cleanup EXIT
stop(){ kill -TERM "$pid";wait "$pid";pid=; }
start(){
  "$server" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" "$@" >> "$root/server.log" 2>&1 & pid=$!
  for _ in $(seq 1 100);do if "$ctl" health > "$root/health.json" 2>/dev/null;then return;fi;kill -0 "$pid";sleep .05;done;return 1
}
reject(){ local name=$1;shift;if "$@" > "$root/$name" 2>&1;then printf 'unexpected acceptance: %s\n' "$name" >&2;exit 1;else test "$?" = 2;fi; }
wait_state(){
  for _ in $(seq 1 200);do "$ctl" status external > "$root/status.json";if jq -e --arg state "$1" '.actual.status==$state' "$root/status.json" >/dev/null;then return;fi;sleep .05;done
  printf 'external pipeline state timeout\n' >&2;return 1
}
start
for role in source sink transform;do
  "$sign" sign "$sample/$role.json" publisher "$root/private.pk8" "$root/$role-signature.json" > "$root/sign-$role.json"
  "$ctl" plugin-install "$sample/$role.json" "$sample/artifact.elf" "$root/$role-signature.json" > "$root/install-$role.json"
done
source_hash=$(jq -er '.manifest_sha256' "$root/install-source.json");sink_hash=$(jq -er '.manifest_sha256' "$root/install-sink.json");v1=$(jq -er '.manifest_sha256' "$root/install-transform.json")
reject disabled-switch.json "$ctl" plugin-enable "$v1"
stop
export SPARROW_ENABLE_EXTERNAL_PLUGINS=1
start
for hash in "$source_hash" "$sink_hash" "$v1";do "$ctl" plugin-enable "$hash" > "$root/enable-$hash.json";done
printf '%s\n' '{"fields":[{"name":"value","type":"int64","nullable":false}]}' > "$root/schema.json"
"$ctl" put-stream s "$root/schema.json" > "$root/stream.json"
# Same independently built executable, distinct immutable manifest revision.
# Configured multiplier changes demonstrate binding/rollback, not new code speed.
jq '.version="v2"' "$sample/transform.json" > "$root/transform-v2.json"
"$sign" sign "$root/transform-v2.json" publisher "$root/private.pk8" "$root/v2-signature.json" > "$root/sign-v2.json"
"$ctl" plugin-install "$root/transform-v2.json" "$sample/artifact.elf" "$root/v2-signature.json" > "$root/install-v2.json"
v2=$(jq -er '.manifest_sha256' "$root/install-v2.json");"$ctl" plugin-enable "$v2" > "$root/enable-v2.json"
run(){
  local revision=$1 hash=$2 factor=$3 value=$4 expected=$5 label=$6
  jq -n --arg source "$source_hash" --arg sink "$sink_hash" --arg hash "$hash" --arg revision "$revision" --arg output "$root/$label.ndjson" --argjson factor "$factor" --argjson value "$value" '{stream:"s",source:{kind:"plugin",inbox_capacity:1,plugin:{name:"example_source",version:"v1",manifest_sha256:$source,config:{start:$value,count:1}}},sink:{kind:"plugin",outbox_capacity:1,plugin:{name:"example_sink",version:"v1",manifest_sha256:$sink,config:{path:$output}}},graph:{version:1,pipeline_id:1,revision_id:1,nodes:[{id:1,kind:"memory_source",table:"s",out:[2]},{id:2,kind:"plugin_transform",plugin:{name:"example_transform",version:$revision,manifest_sha256:$hash,config:{factor:$factor,copies:1}},out:[3]},{id:3,kind:"capture_sink"}]}}' > "$root/pipeline.json"
  local options=();if [[ -n "$etag" ]];then options=(--if-match "$etag");fi
  "$ctl" put-pipeline external "$root/pipeline.json" "${options[@]}" > "$root/put-$label.json";etag=$(jq -er '.etag' "$root/put-$label.json")
  "$ctl" start external > "$root/start-$label.json"
  if [[ "$expected" == fail ]];then
    wait_state failed;cp "$root/status.json" "$root/failed.json"
    sleep 2;"$ctl" status external > "$root/failed-later.json"
    test "$(jq -er '.actual.attempt_id' "$root/failed.json")" = "$(jq -er '.actual.attempt_id' "$root/failed-later.json")"
    test ! -s "$root/$label.ndjson"
  else
    wait_state completed;jq -s -e --argjson expected "$expected" '.==[{value:$expected}]' "$root/$label.ndjson" >/dev/null
  fi
  "$ctl" plugins > "$root/after-$label.json";jq -e '.external_sessions==0 and (.packages|all(.[];.pins==0))' "$root/after-$label.json" >/dev/null
  "$ctl" stop external > "$root/stop-$label.json";wait_state stopped
}
run v1 "$v1" 2 21 42 v1
run v2 "$v2" 3 21 63 v2
run v1 "$v1" 2 21 42 rollback
run v1 "$v1" 9223372036854775807 21 fail overflow
"$ctl" health > "$root/healthy-after-failure.json"
"$ctl" plugin-references "$v1" > "$root/references.json";jq -e '.count==3' "$root/references.json" >/dev/null
reject pinned-catalog.json "$ctl" plugin-uninstall "$v1"
stop
start --safe-mode
"$ctl" plugins > "$root/safe-mode.json";jq -e '(.external_allowed|not) and .external_sessions==0 and (.packages|all(.[];(.enabled|not)))' "$root/safe-mode.json" >/dev/null
reject safe-enable.json "$ctl" plugin-enable "$v1"
stop
start
"$ctl" plugins > "$root/restarted.json";jq -e '.external_allowed and .external_sessions==0 and (.packages|all(.[];.enabled and .signature_verified))' "$root/restarted.json" >/dev/null
"$ctl" retire-pipeline external "$etag" > "$root/retired.json"
for hash in "$v1" "$v2" "$source_hash" "$sink_hash";do "$ctl" plugin-disable "$hash" > "$root/disable-$hash.json";"$ctl" plugin-uninstall "$hash" > "$root/uninstall-$hash.json";done
"$ctl" plugins > "$root/empty.json";jq -e '.external_sessions==0 and (.packages|length)==0' "$root/empty.json" >/dev/null
stop
jq -n '{passed:true,source_transform_sink:true,bindings:[42,63,42],signed:true,default_off:true,overflow_no_partial_output:true,no_automatic_retry:true,persistent_references:true,safe_mode:true,restart:true,reaped:true}' > "$root/result.json"
printf 'EXTERNAL_PLUGINS_PROCESS_OK\n'

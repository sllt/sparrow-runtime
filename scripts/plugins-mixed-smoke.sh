#!/usr/bin/env bash
# Bounded mixed-backend repetition; not a target-capacity or 24h soak claim.
set -euo pipefail
umask 077
unset SPARROW_SAFE_MODE SPARROW_PLUGIN_TRUST_STORE
server=$(realpath "${1:?server}");ctl=$(realpath "${2:?ctl}");sample=$(realpath "${3:?extension package}");root=${4:?new evidence directory}
repo=$(cd "$(dirname "$0")/.."&&pwd);test ! -e "$root";mkdir -p "$root";root=$(cd "$root"&&pwd)
export SPARROW_WASM_PACK=${SPARROW_WASM_PACK:-$(dirname "$server")/sparrow-wasm-pack}
export SPARROW_JS_WORKER=${SPARROW_JS_WORKER:-$(dirname "$server")/sparrow-js-worker}
export SPARROW_WASM_WORKER=${SPARROW_WASM_WORKER:-$(dirname "$server")/sparrow-wasm-worker}
for backend in native script wasm;do bash "$repo/scripts/build-$backend-plugin-example.sh" "$root/$backend" > "$root/build-$backend.log" 2>&1;done
port=$((32000+$$%3000));test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_TOKEN=mixed-test-not-a-deployment-secret SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef SPARROW_URL="http://127.0.0.1:$port"
export SPARROW_PLUGIN_DIR="$root/plugins" SPARROW_DATA_ROOTS="$root" SPARROW_ENABLE_NATIVE_PLUGINS=1 SPARROW_ENABLE_SCRIPT_PLUGINS=1 SPARROW_ENABLE_WASM_PLUGINS=1 SPARROW_ENABLE_EXTERNAL_PLUGINS=1
pid=;cleanup(){ if [[ -n "$pid" ]];then kill "$pid" 2>/dev/null||true;wait "$pid" 2>/dev/null||true;fi; };trap cleanup EXIT
"$server" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" > "$root/server.log" 2>&1 & pid=$!
ready=0;for _ in $(seq 1 100);do if "$ctl" health > "$root/health.json" 2>/dev/null;then ready=1;break;fi;kill -0 "$pid";sleep .05;done;test "$ready" = 1
install(){ "$ctl" plugin-install "$1" "$2" > "$root/install-$3.json";local hash;hash=$(jq -er '.manifest_sha256' "$root/install-$3.json");"$ctl" plugin-enable "$hash" > "$root/enable-$3.json"; }
install "$root/native/manifest.json" "$root/native/native_math.so" native
install "$root/script/manifest.json" "$root/script/script_math.js" script
install "$root/wasm/manifest.json" "$root/wasm/wasm_math.wasm" wasm
for role in source sink transform;do install "$sample/$role.json" "$sample/artifact.elf" "$role";done
printf '%s\n' '{"fields":[{"name":"value","type":"int64","nullable":false}]}' > "$root/schema.json"
"$ctl" put-stream s "$root/schema.json" > "$root/stream.json"
etag=;start_seconds=$SECONDS
for round in $(seq 1 10);do
  jq -n --slurpfile native "$root/install-native.json" --slurpfile js "$root/install-script.json" --slurpfile wasm "$root/install-wasm.json" --slurpfile src "$root/install-source.json" --slurpfile sink "$root/install-sink.json" --slurpfile transform "$root/install-transform.json" --arg output "$root/rows-$round.ndjson" '
    def binding($i;$config): {name:$i.manifest.name,version:$i.manifest.version,manifest_sha256:$i.manifest_sha256,config:$config};
    def lit($s): {k:"lit",value:{t:"utf8",v:$s}};
    def call($i;$input): {k:"call",name:"plugin_call",args:[lit($i.manifest.name),lit($i.manifest.version),lit($i.manifest_sha256),lit("double"),$input]};
    {stream:"s",source:{kind:"plugin",inbox_capacity:1,plugin:binding($src[0];{start:1,count:2048})},sink:{kind:"plugin",outbox_capacity:1,plugin:binding($sink[0];{path:$output})},graph:{version:1,pipeline_id:1,revision_id:1,nodes:[{id:1,kind:"memory_source",table:"s",out:[2]},{id:2,kind:"project",exprs:[{alias:"value",expr:call($native[0];call($wasm[0];call($js[0];{k:"col",name:"value"})))}],out:[3]},{id:3,kind:"plugin_transform",plugin:binding($transform[0];{factor:2,copies:1}),out:[4]},{id:4,kind:"capture_sink"}]}}' > "$root/pipeline.json"
  options=();if [[ -n "$etag" ]];then options=(--if-match "$etag");fi
  "$ctl" put-pipeline mixed "$root/pipeline.json" "${options[@]}" > "$root/put-$round.json";etag=$(jq -er '.etag' "$root/put-$round.json")
  "$ctl" start mixed > "$root/start-$round.json"
  complete=0
  for _ in $(seq 1 600);do
    "$ctl" status mixed > "$root/status-$round.json"
    state=$(jq -er '.actual.status' "$root/status-$round.json")
    if [[ "$state" == completed ]];then complete=1;break;fi
    test "$state" != failed;sleep .05
  done
  test "$complete" = 1
  jq -s -e 'length==2048 and all(to_entries[];.value.value==((.key+1)*16))' "$root/rows-$round.ndjson" >/dev/null
  "$ctl" plugins > "$root/plugins-$round.json"
  jq -e '.external_sessions==0 and .isolated_worker_slots==2 and (.packages|all(.[];.pins==0))' "$root/plugins-$round.json" >/dev/null
  awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" > "$root/rss-$round.kib"
  ps -o pid,ppid,rss,comm --ppid "$pid" > "$root/children-$round.txt"
  "$ctl" stop mixed > "$root/stop-$round.json"
  for _ in $(seq 1 100);do "$ctl" status mixed > "$root/stopped.json";if jq -e '.actual.status=="stopped"' "$root/stopped.json" >/dev/null;then break;fi;sleep .05;done
  jq -e '.actual.status=="stopped"' "$root/stopped.json" >/dev/null
done
first=$(sed -n '1p' "$root/rss-1.kib");last=$(sed -n '1p' "$root/rss-10.kib")
test "$((last-first))" -lt 32768
"$ctl" retire-pipeline mixed "$etag" > "$root/retire.json"
for kind in source sink transform script wasm native;do hash=$(jq -er '.manifest_sha256' "$root/install-$kind.json");"$ctl" plugin-disable "$hash" > "$root/disable-$kind.json";done
"$ctl" plugins > "$root/disabled.json";jq -e '.external_sessions==0 and .isolated_worker_slots==0' "$root/disabled.json" >/dev/null
kill -TERM "$pid";wait "$pid";pid=
jq -n --argjson elapsed "$((SECONDS-start_seconds))" --argjson first "$first" --argjson last "$last" '{passed:true,rounds:10,rows:20480,factor:16,elapsed_seconds:$elapsed,server_rss_first_kib:$first,server_rss_last_kib:$last,external_sessions_after_each:0,scalar_workers_after_each:2,workers_after_disable:0,claim:"bounded_repetition_not_capacity_or_long_soak"}' > "$root/result.json"
printf 'MIXED_PLUGINS_PROCESS_OK\n'

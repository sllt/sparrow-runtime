#!/usr/bin/env bash
# Real, isolated Server/CLI oracle. Takes PREBUILT binaries; does not compile.
# Usage: lookup-process-smoke.sh SERVER CTL LOOKUP_FIXTURE NEW_EVIDENCE
set -euo pipefail
umask 077
server=$(realpath "${1:?server binary}");ctl=$(realpath "${2:?ctl binary}")
fixture=$(realpath "${3:?prebuilt lookup-process fixture}");root=${4:?new evidence directory}
test -x "$server";test -x "$ctl";test -x "$fixture";test ! -e "$root"
for command in jq curl ss sha256sum;do command -v "$command" >/dev/null;done
mkdir -p "$root";root=$(cd "$root"&&pwd)
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
unset SPARROW_PLUGIN_DIR SPARROW_PLUGIN_TRUST_STORE SPARROW_SAFE_MODE SPARROW_SECRETS_KEY_FILE
export SPARROW_ENABLE_NATIVE_PLUGINS=0 SPARROW_ENABLE_SCRIPT_PLUGINS=0 SPARROW_ENABLE_WASM_PLUGINS=0 SPARROW_ENABLE_EXTERNAL_PLUGINS=0
export SPARROW_TOKEN=lookup-process-not-a-deployment-secret
export SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef SPARROW_REQUIRE_SECRETS_KEY=1 SPARROW_DATA_ROOTS="$root"
server_pid=;fixture_pid=
terminate(){
  local pid=$1
  kill -TERM "$pid" 2>/dev/null||true
  for _ in $(seq 1 100);do if ! kill -0 "$pid" 2>/dev/null;then wait "$pid";return;fi;sleep .05;done
  kill -KILL "$pid" 2>/dev/null||true;wait "$pid" 2>/dev/null||true
  printf 'process did not exit after TERM: %s\n' "$pid" >&2;return 1
}
cleanup(){
  local result=$?
  trap - EXIT
  if [[ -n "$server_pid" ]];then terminate "$server_pid"||result=1;fi
  if [[ -n "$fixture_pid" ]];then terminate "$fixture_pid"||result=1;fi
  printf '%s\n' "$result" > "$root/process.exit"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sha256sum "$server" "$ctl" "$fixture" > "$root/binaries.sha256"
"$fixture" --dir "$root/fixture" > "$root/fixture.log" 2>&1 & fixture_pid=$!
ready=0
for _ in $(seq 1 100);do
  if [[ -s "$root/fixture/ports.json" ]];then ready=1;break;fi
  kill -0 "$fixture_pid";sleep .05
done
test "$ready" = 1
fixture_url=$(jq -er '.url' "$root/fixture/ports.json");fixture_port=$(jq -er '.port' "$root/fixture/ports.json")
port=$((35000+$$%3000));test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_URL="http://127.0.0.1:$port"
cli(){
  local label=$1;shift
  printf '%s ' "$label" >> "$root/commands.log";printf '%q ' "$ctl" --timeout-ms 2000 "$@" >> "$root/commands.log";printf '\n' >> "$root/commands.log"
  local code=0
  "$ctl" --timeout-ms 2000 "$@" > "$root/$label.json" 2> "$root/$label.stderr"||code=$?
  printf '%s %s\n' "$label" "$code" >> "$root/cli-exits.log"
  return "$code"
}
reject_cli(){
  local label=$1;shift;local code=0
  cli "$label" "$@"||code=$?
  test "$code" = 2
  jq -e '.error.code|type=="string"' "$root/$label.json" >/dev/null
}
api(){
  local label=$1 method=$2 url=$3 file=${4:-} expected=${5:-2xx};local args=()
  if [[ -n "$file" ]];then args=(-H 'Content-Type: application/json' --data-binary "@$file");fi
  if [[ "$url" == "$SPARROW_URL"* ]];then args+=(-H "Authorization: Bearer $SPARROW_TOKEN");fi
  printf '%s %s %s %s\n' "$label" "$method" "$url" "$file" >> "$root/http-requests.log"
  curl --silent --show-error --noproxy '*' --connect-timeout 1 --max-time 2 -X "$method" "${args[@]}" -o "$root/$label.json" -w '%{http_code}\n' "$url" > "$root/$label.http-status"
  local status
  status=$(sed -n '1p' "$root/$label.http-status")
  if [[ "$expected" == 2xx ]];then [[ "$status" == 2* ]];else test "$status" = "$expected";fi
}
start_server(){
  "$server" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" --max-jobs 4 >> "$root/server.log" 2>&1 & server_pid=$!
  for poll in $(seq 1 100);do
    if cli "health-$server_pid-$poll" health;then return;fi
    kill -0 "$server_pid";sleep .05
  done
  printf 'server readiness deadline\n' >&2;return 1
}
stop_server(){ terminate "$server_pid";server_pid=; }
wait_state(){
  local name=$1 expected=$2 label=$3
  for poll in $(seq 1 150);do
    cli "$label-$poll" status "$name"
    if jq -e --arg status "$expected" '.actual.status==$status' "$root/$label-$poll.json" >/dev/null;then cp "$root/$label-$poll.json" "$root/$label.json";return;fi
    sleep .05
  done
  printf 'state deadline: %s -> %s\n' "$name" "$expected" >&2;return 1
}
wait_revision(){
  local revision=$1 label=$2
  for poll in $(seq 1 150);do
    cli "$label-$poll" status live
    if jq -e --argjson revision "$revision" '.actual.status=="running" and .lookup_runtime.live_tables.limits.observed_revision==$revision' "$root/$label-$poll.json" >/dev/null;then
      cp "$root/$label-$poll.json" "$root/$label.json"
      cli "$label-catalog" table limits "$revision"
      jq -e -s '.[0].lookup_runtime.live_tables.limits.sha256==.[1].sha256' "$root/$label.json" "$root/$label-catalog.json" >/dev/null
      return
    fi
    sleep .05
  done
  printf 'live revision deadline: %s\n' "$revision" >&2;return 1
}
snapshot(){ api "$1" GET "$fixture_url/result"; }
expect_row(){
  local sequence=$1 device=$2 threshold=$3
  for poll in $(seq 1 150);do
    snapshot "rows-$sequence-$poll"
    if jq -e --argjson sequence "$sequence" --argjson device "$device" --argjson threshold "$threshold" '[.rows[]|select(.seq==$sequence)] as $r|($r|length)==1 and $r[0].device_id==$device and $r[0].threshold==$threshold' "$root/rows-$sequence-$poll.json" >/dev/null;then
      jq -cn --argjson sequence "$sequence" --argjson device "$device" --argjson threshold "$threshold" '{seq:$sequence,device_id:$device,threshold:$threshold}' >> "$root/expected.ndjson"
      return
    fi
    sleep .05
  done
  printf 'output oracle deadline: seq=%s threshold=%s\n' "$sequence" "$threshold" >&2;return 1
}
append_row(){ jq -cn --argjson seq "$2" --argjson device "$3" '{device_id:$device,seq:$seq}' >> "$root/$1.ndjson"; }
stop_pipeline(){ cli "stop-$1" stop "$1";wait_state "$1" stopped "stopped-$1"; }
held(){
  local name=$1 label=$2
  cli "$label-before" status "$name";sleep 1;cli "$label-after" status "$name"
  jq -e -s '.[0].actual.attempt_id==.[1].actual.attempt_id and .[1].actual.restart_blocked and .[1].actual.status!="running"' "$root/$label-before.json" "$root/$label-after.json" >/dev/null
}
fixture_update(){
  local label=$1 mode=$2 threshold=$3 delay=${4:-0}
  jq -n --arg mode "$mode" --argjson threshold "$threshold" --argjson delay "$delay" '{mode:$mode,threshold:$threshold,delay_ms:$delay}' > "$root/$label.request.json"
  api "$label" POST "$fixture_url/update" "$root/$label.request.json"
}
base_spec(){
  local name=$1
  : > "$root/$name.ndjson"
  jq -n --arg path "$root/$name.ndjson" --arg url "$fixture_url/ingest" '{stream:"sensors",source:{kind:"file",path:$path,file_contract:"append_only",inbox_capacity:4},sink:{kind:"http",url:$url,batch_rows:1,batch_bytes:8192,linger_ms:0,max_inflight:1,outbox_capacity:4},delivery:"live_best_effort",recovery:"restart_fresh"}'
}
graph(){ jq '.graph={version:1,pipeline_id:1,revision_id:1,nodes:[{id:1,kind:"memory_source",table:"sensors",out:[2]},{id:2,kind:"lookup",table:"limits",on:[{stream:"device_id",table:"device_id"}],keep:["threshold"],out:[3]},{id:3,kind:"capture_sink"}]}' ; }
external_spec(){
  local name=$1 style=$2 policy=$3 ttl=$4
  base_spec "$name" | jq --arg url "$fixture_url/lookup" --arg policy "$policy" --argjson ttl "$ttl" '{
    stream,source,sink,delivery,recovery,
    external_lookups:{remote:{url:$url,fields:[{name:"device_id",type:"utf8",nullable:false},{name:"threshold",type:"int64",nullable:false}],keys:["device_id"],options:{max_inflight:2,timeout_ms:500,cache_ttl_ms:$ttl,cache_bytes:4096,on_error:$policy}}}
  }' > "$root/$name.base.json"
  if [[ "$style" == sql ]];then
    jq '.sql="SELECT * FROM sensors LEFT JOIN remote ON sensors.device_id = remote.device_id"' "$root/$name.base.json"
  else
    graph < "$root/$name.base.json" | jq '.graph.nodes[1].table="remote"'
  fi
}
publish_pipeline(){
  local name=$1
  cli "validate-$name" validate "$root/$name.spec.json"
  cli "put-$name" put-pipeline "$name" "$root/$name.spec.json"
  cli "start-$name" start "$name";wait_state "$name" running "running-$name"
}

start_server
jq -n --argjson port "$fixture_port" '{host:"127.0.0.1",port:$port}' > "$root/allow.request.json"
api allow PUT "$SPARROW_URL/v1/allowlist" "$root/allow.request.json"
printf '%s\n' '{"fields":[{"name":"device_id","type":"utf8","nullable":true},{"name":"seq","type":"int64","nullable":false}]}' > "$root/schema.json"
cli stream put-stream sensors "$root/schema.json"
printf '%s\n' '{"expected_revision":0,"table":{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"threshold","type":"int64","nullable":false}],"keys":["device_id"],"rows":[["d1",10],["d2",20]]}}' > "$root/table-r1.request.json"
cli table-r1 put-table limits "$root/table-r1.request.json"
jq -e '.revision==1 and .row_count==2' "$root/table-r1.json" >/dev/null
sha1=$(jq -er '.sha256' "$root/table-r1.json")
base_spec live | jq --arg sha "$sha1" '.reference_tables={limits:{revision:1,sha256:$sha,follow_latest:true}}|.sql="SELECT * FROM sensors LEFT JOIN limits ON sensors.device_id = limits.device_id"' > "$root/live.spec.json"
base_spec pinned | jq --arg sha "$sha1" '.reference_tables={limits:{revision:1,sha256:$sha}}' | graph > "$root/pinned.spec.json"
publish_pipeline live;publish_pipeline pinned;wait_revision 1 observed-r1
append_row live 1 '"d1"';expect_row 1 '"d1"' 10
append_row pinned 101 '"d1"';expect_row 101 '"d1"' 10
printf '%s\n' '{"expected_revision":1,"operations":[{"op":"upsert","row":["d1",15]},{"op":"upsert","row":["d3",30]}]}' > "$root/mutate-r2.request.json"
cli mutate-r2 mutate-table limits "$root/mutate-r2.request.json";jq -e '.revision==2 and .row_count==3' "$root/mutate-r2.json" >/dev/null
wait_revision 2 observed-r2
append_row live 2 '"d1"';expect_row 2 '"d1"' 15
append_row live 3 '"d3"';expect_row 3 '"d3"' 30
append_row pinned 102 '"d1"';expect_row 102 '"d1"' 10
reject_cli stale-cas mutate-table limits "$root/mutate-r2.request.json"
api stale-cas-http POST "$SPARROW_URL/v1/tables/limits/mutate" "$root/mutate-r2.request.json" 412
jq -e '(.error.context|from_entries) as $c|$c.expected_revision=="1" and $c.current_revision=="2"' "$root/stale-cas-http.json" >/dev/null
cli head-after-cas table limits;jq -e '.revision==2 and .table.rows==[["d1",15],["d2",20],["d3",30]]' "$root/head-after-cas.json" >/dev/null
printf '%s\n' '{"expected_revision":2,"operations":[{"op":"delete","key":["d1"]}]}' > "$root/mutate-r3.request.json"
cli mutate-r3 mutate-table limits "$root/mutate-r3.request.json";jq -e '.revision==3 and .row_count==2' "$root/mutate-r3.json" >/dev/null
wait_revision 3 observed-r3;append_row live 4 '"d1"';expect_row 4 '"d1"' null
cli rollback-r4 rollback-table limits 3 1;jq -e '.revision==4 and .row_count==2' "$root/rollback-r4.json" >/dev/null
wait_revision 4 observed-r4;append_row live 5 '"d1"';expect_row 5 '"d1"' 10
cli pinned-still-r1 status pinned;jq -e '.reference_tables.running_actual.bindings.limits.revision==1' "$root/pinned-still-r1.json" >/dev/null
stop_pipeline pinned

# A valid catalog publication can still be incompatible with a running Job.
jq '.expected_revision=4|.table.fields[1].type="utf8"|.table.rows=[["d1","changed-type"]]' "$root/table-r1.request.json" > "$root/incompatible-r5.request.json"
cli incompatible-r5 put-table limits "$root/incompatible-r5.request.json";jq -e '.revision==5' "$root/incompatible-r5.json" >/dev/null
wait_state live failed live-refresh-failed;held live incompatible-held
jq '.expected_revision=5|.table.rows=[["d1",55],["d2",25]]' "$root/table-r1.request.json" > "$root/compatible-r6.request.json"
cli compatible-r6 put-table limits "$root/compatible-r6.request.json";jq -e '.revision==6' "$root/compatible-r6.json" >/dev/null
held live compatible-still-held
: > "$root/live.ndjson";append_row live 6 '"d1"'
cli live-explicit-restart start live;wait_revision 6 observed-r6;expect_row 6 '"d1"' 55
stop_server
# The file is reset only while its actor/process is gone: fresh-only, not replay.
: > "$root/live.ndjson";append_row live 7 '"d1"'
start_server;wait_state live stopped live-restart-gate;held live restart-held
snapshot before-explicit-restart
jq -e '[.rows[]|select(.seq==7)]|length==0' "$root/before-explicit-restart.json" >/dev/null
cli live-start-after-process-restart start live;wait_revision 6 observed-r6-after-restart;expect_row 7 '"d1"' 55
stop_pipeline live
cli table-history table-revisions limits;cli table-dependencies table-dependencies limits;cli table-gc gc-table limits
cli pinned-history-survives table limits 1;jq -e '.revision==1 and .table.rows==[["d1",10],["d2",20]]' "$root/pinned-history-survives.json" >/dev/null

external_spec external sql fail 2000 > "$root/external.spec.json"
publish_pipeline external;snapshot ext-baseline
requests=$(jq -er '.stats.requests' "$root/ext-baseline.json")
append_row external 201 '"d1"';expect_row 201 '"d1"' 100;snapshot ext-first
test "$(jq -er '.stats.requests' "$root/ext-first.json")" = "$((requests+1))"
fixture_update fixture-110 ok 110
append_row external 202 '"d1"';expect_row 202 '"d1"' 100;snapshot ext-positive-cache
test "$(jq -er '.stats.requests' "$root/ext-positive-cache.json")" = "$((requests+1))"
append_row external 203 '"missing"';expect_row 203 '"missing"' null
append_row external 204 '"missing"';expect_row 204 '"missing"' null
append_row external 205 null;expect_row 205 null null;snapshot ext-negative-cache
test "$(jq -er '.stats.requests' "$root/ext-negative-cache.json")" = "$((requests+2))"
sleep 2.2;append_row external 206 '"d1"';expect_row 206 '"d1"' 110;snapshot ext-ttl
test "$(jq -er '.stats.requests' "$root/ext-ttl.json")" = "$((requests+3))"
cli external-diagnostics status external
jq -e '.lookup_runtime.external.remote as $s|$s.cache_hits>=2 and $s.negative_cache_hits>=1 and $s.null_keys==1 and $s.peak_inflight<=2 and $s.cache_bytes<=4096 and $s.inflight==0' "$root/external-diagnostics.json" >/dev/null
stop_pipeline external

external_spec soft graph null 2000 > "$root/soft.spec.json"
fixture_update fixture-error error 120;publish_pipeline soft
append_row soft 301 '"d1"';expect_row 301 '"d1"' null;snapshot soft-failed-request
requests=$(jq -er '.stats.requests' "$root/soft-failed-request.json")
fixture_update fixture-recover ok 120
append_row soft 302 '"d1"';expect_row 302 '"d1"' 120;snapshot soft-not-cached
test "$(jq -er '.stats.requests' "$root/soft-not-cached.json")" = "$((requests+1))"
cli soft-diagnostics status soft;jq -e '.lookup_runtime.external.remote.error_nulls==1 and .lookup_runtime.external.remote.failures==1' "$root/soft-diagnostics.json" >/dev/null
fixture_update fixture-wrong-schema wrong_schema 120
append_row soft 303 '"d2"';wait_state soft failed soft-protocol-failed;held soft protocol-held;stop_pipeline soft

external_spec oversized graph null 0 > "$root/oversized.spec.json"
fixture_update fixture-oversized oversized 120;publish_pipeline oversized
append_row oversized 401 '"d1"';wait_state oversized failed oversized-hard-failed;held oversized oversized-held;stop_pipeline oversized
external_spec strict graph fail 0 > "$root/strict.spec.json"
fixture_update fixture-strict-error error 120;publish_pipeline strict
append_row strict 501 '"d1"';wait_state strict failed strict-transport-failed;held strict strict-held;stop_pipeline strict

# Three rows written atomically exercise a bounded parallel request window,
# including a miss whose completion must not reorder the output sequence.
external_spec parallel graph fail 0 > "$root/parallel.spec.json"
fixture_update fixture-parallel ok 130 80;publish_pipeline parallel
jq -cn '{device_id:"d1",seq:601},{device_id:"d2",seq:602},{device_id:"missing",seq:603}' >> "$root/parallel.ndjson"
expect_row 601 '"d1"' 130;expect_row 602 '"d2"' 140;expect_row 603 '"missing"' null
cli parallel-diagnostics status parallel
jq -e '.lookup_runtime.external.remote.peak_inflight==2 and .lookup_runtime.external.remote.inflight==0' "$root/parallel-diagnostics.json" >/dev/null
snapshot parallel-result
jq -e '[.rows[]|select(.seq>=601 and .seq<=603)|.seq]==[601,602,603] and .stats.peak==2' "$root/parallel-result.json" >/dev/null
stop_pipeline parallel

# Stop must cancel a pending provider request, not publish on_error NULL.
external_spec cancelled graph null 0 > "$root/cancelled.spec.json"
fixture_update fixture-cancel ok 140 2000;publish_pipeline cancelled
append_row cancelled 701 '"d1"'
active=0
for poll in $(seq 1 100);do
  snapshot "cancel-pending-$poll"
  if jq -e '.stats.inflight>0' "$root/cancel-pending-$poll.json" >/dev/null;then active=1;break;fi
  sleep .01
done
test "$active" = 1
stop_pipeline cancelled
idle=0
for poll in $(seq 1 100);do
  snapshot "cancel-reaped-$poll"
  if jq -e '.stats.inflight==0' "$root/cancel-reaped-$poll.json" >/dev/null;then idle=1;break;fi
  sleep .01
done
test "$idle" = 1
fixture_update fixture-final ok 140

# Admission refuses live-dependent aligned replay and an unallowlisted target.
jq --arg dir "$root/denied-checkpoint" '.recovery="aligned"|.checkpoint_dir=$dir' "$root/external.spec.json" > "$root/denied-aligned.spec.json"
reject_cli denied-external-aligned validate "$root/denied-aligned.spec.json"
jq -e '.error.code=="unsupported_restore"' "$root/denied-external-aligned.json" >/dev/null
jq --arg dir "$root/denied-live-checkpoint" '.recovery="aligned"|.checkpoint_dir=$dir' "$root/live.spec.json" > "$root/denied-live.spec.json"
reject_cli denied-live-aligned validate "$root/denied-live.spec.json"
jq -e '.error.code=="unsupported_restore"' "$root/denied-live-aligned.json" >/dev/null
jq '.external_lookups.remote.url="http://127.0.0.1:1/lookup"' "$root/external.spec.json" > "$root/denied-policy.spec.json"
reject_cli denied-external-policy validate "$root/denied-policy.spec.json"
jq -e '.error.code=="policy_denied"' "$root/denied-external-policy.json" >/dev/null
test ! -e "$root/denied-checkpoint";test ! -e "$root/denied-live-checkpoint"

snapshot final-fixture
jq -s -e '(.[0].rows|sort_by(.seq))==(.[1:]|sort_by(.seq))' "$root/final-fixture.json" "$root/expected.ndjson" >/dev/null
jq -e '.stats.inflight==0 and .stats.peak<=2 and (.stats.overflow|not) and (.rows|type)=="array"' "$root/final-fixture.json" >/dev/null
cli healthy-final health
credits_reaped=0
for poll in $(seq 1 100);do
  api "credits-final-$poll" GET "$SPARROW_URL/v1/metrics"
  if jq -e '.process_credits as $c|$c.reservation_bytes==0 and $c.retention_bytes==0 and $c.queue_bytes==0 and $c.physical_bytes==0 and $c.live_handles==0' "$root/credits-final-$poll.json" >/dev/null;then
    credits_reaped=1;cp "$root/credits-final-$poll.json" "$root/credits-final.json";break
  fi
  sleep .05
done
test "$credits_reaped" = 1
stop_server
terminate "$fixture_pid";fixture_pid=
jq -n --slurpfile fixture "$root/final-fixture.json" '{passed:true,sql_and_graph:true,incremental_upsert_delete:true,rollback_new_revision:4,live_final_revision:6,static_pin:1,cas_rejected:true,incompatible_schema_hard_failure:true,no_automatic_restart:true,explicit_restart_gate:true,external_hit_miss_null_key:true,positive_negative_cache:true,ttl_refresh:true,transport_error_null_not_cached:true,protocol_limit_not_softened:true,parallel_ordered:true,stop_cancels_pending:true,aligned_policy_rejected:true,reaped:true,rows:($fixture[0].rows|length),lookup_requests:$fixture[0].stats.requests,peak_inflight:$fixture[0].stats.peak,claim:"bounded_process_acceptance_not_soak_or_capacity"}' > "$root/result.json"
printf 'LOOKUP_PROCESS_OK\n'

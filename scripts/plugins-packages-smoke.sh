#!/usr/bin/env bash
# Isolated catalog and test keys only; no existing service/trust-store changes.
set -euo pipefail
umask 077
unset SPARROW_SAFE_MODE
server=$(realpath "${1:?server}");ctl=$(realpath "${2:?ctl}");sign=$(realpath "${3:?signing utility}");root=${4:?new evidence directory}
test ! -e "$root";mkdir -p "$root";root=$(cd "$root"&&pwd)
repo=$(cd "$(dirname "$0")/.."&&pwd)
port=$((27000+$$%4000));test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_TOKEN=packages-test-not-a-deployment-secret SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef
export SPARROW_PLUGIN_DIR="$root/plugins" SPARROW_ENABLE_NATIVE_PLUGINS=0 SPARROW_ENABLE_SCRIPT_PLUGINS=1 SPARROW_ENABLE_WASM_PLUGINS=0
export SPARROW_PLUGIN_TRUST_STORE="$root/trust.json" SPARROW_DATA_ROOTS="$root" SPARROW_URL="http://127.0.0.1:$port"
"$sign" keygen "$root/private.pk8" "$root/public.json" > "$root/keygen.json"
jq '{format:1,require_signed:true,publishers:[{id:"publisher",public_key_base64:.public_key_base64,packages:["script_math","helper_math","legacy_math"],kinds:["javascript_scalar"],revoked:false}]}' "$root/public.json" > "$root/trust.json"
bash "$repo/scripts/build-script-plugin-example.sh" "$root/helper"
jq '.name="helper_math"' "$root/helper/manifest.json" > "$root/helper-manifest.json"
pid=
cleanup(){ if [[ -n "$pid" ]];then kill "$pid" 2>/dev/null||true;wait "$pid" 2>/dev/null||true;fi; }
trap cleanup EXIT
stop(){ kill -TERM "$pid";wait "$pid";pid=; }
start(){
  "$server" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" "$@" >> "$root/server.log" 2>&1 & pid=$!
  for _ in $(seq 1 100);do if "$ctl" health > "$root/health.json" 2>/dev/null;then return;fi;kill -0 "$pid";sleep .05;done
  return 1
}
reject(){ local name=$1;shift;if "$@" > "$root/$name" 2>&1;then printf 'unexpected acceptance: %s\n' "$name" >&2;exit 1;else test "$?" = 2;fi; }
start
reject unsigned.json "$ctl" plugin-install "$root/helper-manifest.json" "$root/helper/script_math.js"
"$sign" sign "$root/helper-manifest.json" publisher "$root/private.pk8" "$root/helper-signature.json" > "$root/sign-helper.json"
"$sign" verify "$root/helper-manifest.json" "$root/trust.json" "$root/helper-signature.json" > "$root/verify-helper.json"
"$ctl" plugin-install "$root/helper-manifest.json" "$root/helper/script_math.js" "$root/helper-signature.json" > "$root/install-helper.json"
helper=$(jq -er '.manifest_sha256' "$root/install-helper.json")
jq --arg h "$helper" '.format=2 | .package={dependencies:[{name:"helper_math",version:"v1",manifest_sha256:$h}],platform:["linux_gnu","quickjs_ng_0_16_2"]}' "$root/helper/manifest.json" > "$root/parent-manifest.json"
"$sign" sign "$root/parent-manifest.json" publisher "$root/private.pk8" "$root/parent-signature.json" > "$root/sign-parent.json"
reject signature-replay.json "$ctl" plugin-install "$root/parent-manifest.json" "$root/helper/script_math.js" "$root/helper-signature.json"
"$ctl" plugin-install "$root/parent-manifest.json" "$root/helper/script_math.js" "$root/parent-signature.json" > "$root/install-parent.json"
parent=$(jq -er '.manifest_sha256' "$root/install-parent.json")
reject disabled-dependency.json "$ctl" plugin-enable "$parent"
"$ctl" plugin-enable "$helper" > "$root/enable-helper.json"
"$ctl" plugin-enable "$parent" > "$root/enable-parent.json"
reject dependency-disable.json "$ctl" plugin-disable "$helper"
printf '%s\n' '{"fields":[{"name":"value","type":"int64","nullable":false}]}' > "$root/schema.json"
"$ctl" put-stream s "$root/schema.json" > "$root/stream.json"
jq -n --arg sql "SELECT plugin_call('script_math','v1','$parent','double',value) AS doubled FROM s" '{sql:$sql,inputs:[{stream:"s",rows:[{value:21}]}]}' > "$root/query.json"
"$ctl" query "$root/query.json" > "$root/query-result.json";jq -e '.complete and .rows==[{doubled:42}]' "$root/query-result.json" >/dev/null
printf '{"value":21}\n' > "$root/input.ndjson"
jq --arg input "$root/input.ndjson" '{version:1,stream:"s",sql:.sql,source:{kind:"file",path:$input,file_contract:"append_only"},sink:{kind:"log"},recovery:"restart_fresh"}' "$root/query.json" > "$root/pipeline.json"
"$ctl" put-pipeline pinned "$root/pipeline.json" > "$root/put.json";etag=$(jq -er '.etag' "$root/put.json")
"$ctl" plugin-disable "$parent" > "$root/disable-parent.json"
reject durable-reference.json "$ctl" plugin-uninstall "$parent"
"$ctl" plugin-references "$parent" > "$root/references.json";jq -e '.count==1' "$root/references.json" >/dev/null
reject bad-retirement.json "$ctl" retire-pipeline pinned rev_0
"$ctl" retire-pipeline pinned "$etag" > "$root/retire.json"
"$ctl" plugin-uninstall "$parent" > "$root/uninstall-parent.json"
"$ctl" plugin-disable "$helper" > "$root/disable-helper.json"
"$ctl" plugin-uninstall "$helper" > "$root/uninstall-helper.json"
stop
# Explicitly attest existing bytes without changing their legacy identity.
jq '.require_signed=false' "$root/trust.json" > "$root/trust-next.json";mv "$root/trust-next.json" "$root/trust.json"
jq '.name="legacy_math"' "$root/helper/manifest.json" > "$root/legacy-manifest.json"
start
"$ctl" plugin-install "$root/legacy-manifest.json" "$root/helper/script_math.js" > "$root/install-legacy.json"
legacy=$(jq -er '.manifest_sha256' "$root/install-legacy.json")
"$sign" sign "$root/legacy-manifest.json" publisher "$root/private.pk8" "$root/legacy-signature.json" > "$root/sign-legacy.json"
"$ctl" plugin-attest "$legacy" "$root/legacy-signature.json" > "$root/attest-legacy.json"
jq -e --arg h "$legacy" '.manifest_sha256==$h and .signature_verified' "$root/attest-legacy.json" >/dev/null
"$ctl" plugin-enable "$legacy" > "$root/enable-legacy.json"
stop
jq '.require_signed=true' "$root/trust.json" > "$root/trust-next.json";mv "$root/trust-next.json" "$root/trust.json"
start
"$ctl" plugins > "$root/restarted.json";jq -e '.signature_required and (.packages|all(.[]; .enabled and .signature_verified))' "$root/restarted.json" >/dev/null
stop
jq '.publishers[0].revoked=true' "$root/trust.json" > "$root/trust-next.json";mv "$root/trust-next.json" "$root/trust.json"
set +e
timeout 5 "$server" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" > "$root/revoked.log" 2>&1
code=$?
set -e
test "$code" != 0;test "$code" != 124;test "$code" != 137
printf '%s\n' "$code" > "$root/revoked.exit"
start --safe-mode
"$ctl" plugins > "$root/safe-mode.json";jq -e '.packages|all(.[]; (.enabled|not) and (.signature_verified|not))' "$root/safe-mode.json" >/dev/null
"$ctl" plugin-disable "$legacy" > "$root/disable-legacy.json";"$ctl" plugin-uninstall "$legacy" > "$root/uninstall-legacy.json"
stop
jq -n '{passed:true,signed_query:42,dependency_order:true,signature_replay_rejected:true,durable_reference:true,retirement_cas:true,attestation_identity_preserved:true,revocation_rejected:true,safe_mode:true}' > "$root/result.json"
printf 'package trust/dependency/catalog smoke passed\n'

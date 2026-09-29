#!/usr/bin/env bash
# Exercise the standalone worker without the server's 100ms kill/timeout loop.
set -euo pipefail
umask 077
worker=$(realpath "${1:?worker executable}")
root=${2:?new evidence directory}
for tool in jq sha256sum timeout wc dd grep; do
    command -v "$tool" >/dev/null || { printf 'required tool missing: %s\n' "$tool" >&2; exit 2; }
done
test ! -e "$root"; mkdir -p "$root"; root=$(cd "$root" && pwd)
printf '%s\n' "({double(v){ /(a+)+\$/.test('a'.repeat(40)+'!'); return v; }})" > "$root/limit.js"
hash=$(sha256sum "$root/limit.js"); hash=${hash%% *}
jq -cn --arg hash "$hash" --rawfile source "$root/limit.js" \
  '{op:"Load",source:$source,manifest:{format:1,name:"limit",version:"v1",kind:"javascript_scalar",abi:1,semantics:1,
    target:"javascript-quickjs-ng-0.16.2-v1",artifact_sha256:$hash,deterministic:true,thread_safe:true,null_policy:"propagate",
    functions:[{name:"double",id:1,inputs:["int64"],output:"int64",max_output_bytes:1}]}}' > "$root/load.json"
printf '%s' '{"op":"Call","id":1,"args":[{"t":"Int","v":"1"}]}' > "$root/call.json"
frame(){
    local size header
    size=$(wc -c < "$1")
    printf -v header '\\%03o\\%03o\\%03o\\%03o' "$(((size >> 24) & 255))" "$(((size >> 16) & 255))" "$(((size >> 8) & 255))" "$((size & 255))"
    printf '%b' "$header"
    dd if="$1" status=none
}
{ frame "$root/load.json"; frame "$root/call.json"; } > "$root/request.bin"
set +e
timeout 4 env -i "$worker" --sparrow-js-worker-v1 < "$root/request.bin" > "$root/reply.bin" 2> "$root/worker.log"
code=$?
set -e
printf '%s\n' "$code" > "$root/worker.exit"
grep -a -q '"status":"Ready"' "$root/reply.bin"
# Either the engine interrupted the builtin or the autonomous SIGALRM fired.
# A timeout(1) kill (124/137) is NOT a pass.
if [[ "$code" == 0 ]]; then
    grep -a -q '"status":"Failure"' "$root/reply.bin"
else
    test "$code" = 142
fi
printf '\000\010\000\001' > "$root/oversize.bin"
printf '\000\000\000' > "$root/truncated.bin"
for input in oversize truncated; do
    set +e
    timeout 2 env -i "$worker" --sparrow-js-worker-v1 < "$root/$input.bin" > "$root/$input.reply" 2> "$root/$input.log"
    status=$?
    set -e
    test "$status" = 1
done
jq -n --argjson status "$code" '{passed:true,standalone_exit:$status,server_timeout_used:false,oversize_rejected:true,truncated_rejected:true}' > "$root/result.json"
printf 'script worker standalone limits passed\n'

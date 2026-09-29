#!/usr/bin/env bash
set -euo pipefail
umask 077
root=${1:?new output directory}
pack=${SPARROW_WASM_PACK:?absolute path to sparrow-wasm-pack}
repo=$(cd "$(dirname "$0")/.." && pwd)
test ! -e "$root"; mkdir -p "$root"
"$pack" "$repo/examples/plugins/wasm_math.wat" "$root/wasm_math.wasm"
hash=$(sha256sum "$root/wasm_math.wasm");hash=${hash%% *}
jq -n --arg hash "$hash" '{format:1,name:"wasm_math",version:"v1",kind:"wasm_scalar",abi:1,semantics:1,
  target:"wasm32-sparrow-scalar-v1",artifact_sha256:$hash,deterministic:true,thread_safe:true,null_policy:"propagate",
  functions:[{name:"double",id:1,inputs:["int64"],output:"int64",max_output_bytes:1}]}' > "$root/manifest.json"

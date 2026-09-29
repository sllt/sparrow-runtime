#!/usr/bin/env bash
# No compilation; requires jq and sha256sum (or shasum on macOS).
set -euo pipefail
umask 077
root=$(cd "$(dirname "$0")/.." && pwd)
out=${1:?new package directory required}
test ! -e "$out"
mkdir -m 700 "$out"
cp "$root/examples/plugins/script_math.js" "$out/script_math.js"
if command -v sha256sum >/dev/null; then
    hash=$(sha256sum "$out/script_math.js" | cut -d' ' -f1)
else
    hash=$(shasum -a 256 "$out/script_math.js" | cut -d' ' -f1)
fi
jq -n --arg hash "$hash" '{format:1,name:"script_math",version:"v1",kind:"javascript_scalar",
  abi:1,semantics:1,target:"javascript-quickjs-ng-0.16.2-v1",artifact_sha256:$hash,
  deterministic:true,thread_safe:true,null_policy:"propagate",
  functions:[{name:"double",id:1,inputs:["int64"],output:"int64",max_output_bytes:1},
             {name:"upper",id:2,inputs:["utf8"],output:"utf8",max_output_bytes:4096}]}' > "$out/manifest.json"
printf 'SCRIPT_PACKAGE_OK %s\n' "$out"

#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=${1:?usage: build-native-plugin-example.sh NEW_OUTPUT_DIRECTORY}
[[ $(uname -s) == Linux ]] || { printf 'Linux GNU required\n' >&2; exit 1; }
case $(uname -m) in
  x86_64) target=x86_64-unknown-linux-gnu ;;
  aarch64) target=aarch64-unknown-linux-gnu ;;
  *) printf 'unsupported architecture\n' >&2; exit 1 ;;
esac
mkdir "$out"
cc -std=c11 -O2 -fPIC -shared -Wall -Wextra -Werror -I"$root/sdk/native" \
  "$root/examples/plugins/native_math.c" -o "$out/native_math.so"
digest=$(sha256sum "$out/native_math.so"); digest=${digest%% *}
printf '{"format":1,"name":"native_math","version":"v1","kind":"native_scalar","abi":1,"semantics":1,"target":"%s","artifact_sha256":"%s","deterministic":true,"thread_safe":true,"null_policy":"propagate","functions":[{"name":"double","id":1,"inputs":["int64"],"output":"int64","max_output_bytes":1},{"name":"ascii_upper","id":2,"inputs":["utf8"],"output":"utf8","max_output_bytes":65536}]}\n' "$target" "$digest" > "$out/manifest.json"
printf 'Built %s; inspect/approve the code, then use sparrowctl plugin-install.\n' "$out"

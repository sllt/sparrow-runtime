#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=${1:?usage: build-extension-example.sh NEW_OUTPUT_DIRECTORY}
[[ $(uname -s) == Linux ]] || exit 2
case $(uname -m) in x86_64) target=x86_64-unknown-linux-gnu;;aarch64) target=aarch64-unknown-linux-gnu;;*) exit 2;;esac
mkdir "$out";out=$(cd "$out"&&pwd)
build=${CARGO_TARGET_DIR:-$root/target/extensions};mkdir -p "$build";build=$(cd "$build"&&pwd)
cargo build --locked --release --quiet --manifest-path "$root/examples/extensions/Cargo.toml" --bin sparrow-extension-example --target-dir "$build"
install -m 755 "$build/release/sparrow-extension-example" "$out/artifact.elf"
digest=$(sha256sum "$out/artifact.elf");digest=${digest%% *}
for role in source sink transform;do
  jq -n --arg target "$target" --arg digest "$digest" --arg role "$role" '{format:2,name:("example_"+$role),version:"v1",kind:"native_extension",abi:1,semantics:1,target:$target,artifact_sha256:$digest,deterministic:($role=="transform"),thread_safe:true,null_policy:"typed_rows",functions:[],package:{dependencies:[],platform:["sparrow_abi_v1","linux_gnu"],extension:{role:$role,input:(if $role=="source" then [] else [{name:"value",type:"int64",nullable:true}] end),output:(if $role=="sink" then [] else [{name:"value",type:"int64",nullable:($role=="transform")}] end),limits:{max_rows:16,max_frame_bytes:16384},permissions:(if $role=="sink" then ["filesystem"] else [] end),watermarks:false}}}' > "$out/$role.json"
done
printf 'EXTENSION_EXAMPLES_OK %s\n' "$out"

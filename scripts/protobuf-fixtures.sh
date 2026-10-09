#!/usr/bin/env bash
# Regenerate the protobuf golden fixtures with a pinned, checksum-verified
# protoc (36.2, linux-x86_64). Usage: scripts/protobuf-fixtures.sh [--check]
#   --check  regenerate into a temp dir and fail if any committed fixture
#            differs (the binaries are committed; CI does not need protoc).
set -euo pipefail

VERSION=36.2
ZIP="protoc-${VERSION}-linux-x86_64.zip"
SHA256=121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278
URL="https://github.com/protocolbuffers/protobuf/releases/download/v${VERSION}/${ZIP}"

root="$(cd "$(dirname "$0")/.." && pwd)"
fixtures="$root/crates/sparrow-formats/tests/fixtures/protobuf"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

curl -fsSL "$URL" -o "$work/$ZIP"
echo "$SHA256  $work/$ZIP" | sha256sum -c -
unzip -q "$work/$ZIP" -d "$work/protoc"
protoc="$work/protoc/bin/protoc"
inc="$work/protoc/include"

out="$fixtures"
if [[ "${1:-}" == "--check" ]]; then
  out="$work/out"
  mkdir -p "$out"
fi

cd "$fixtures"
"$protoc" -I . -I "$inc" --include_imports \
  --descriptor_set_out="$out/descriptor_set.pb" telemetry.proto legacy.proto
# Same messages, different descriptor bytes (restore-identity tests).
"$protoc" -I . -I "$inc" --include_imports \
  --descriptor_set_out="$out/descriptor_set_telemetry.pb" telemetry.proto
for f in reading_full reading_min reading_extras reading_merge; do
  "$protoc" -I . -I "$inc" --encode=telemetry.v1.Reading telemetry.proto <"$f.txtpb" >"$out/$f.bin"
done
"$protoc" -I . -I "$inc" --encode=telemetry.v1.Tree telemetry.proto <tree.txtpb >"$out/tree.bin"
"$protoc" -I . --encode=legacy.v1.Legacy legacy.proto <legacy.txtpb >"$out/legacy.bin"

if [[ "${1:-}" == "--check" ]]; then
  for f in "$out"/*; do
    cmp "$f" "$fixtures/$(basename "$f")"
  done
  echo "protobuf fixtures match protoc $VERSION"
fi

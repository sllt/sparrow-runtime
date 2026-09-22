#!/usr/bin/env bash
# Build only the two production entrypoints. Do not feature-unify with demos.
# Usage: bash scripts/production-build.sh NEW_PACKAGE_DIRECTORY
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
out=${1:?new package directory required}
mode=${SPARROW_BUILD_MODE:-candidate}
jetstream=${SPARROW_JETSTREAM:-0}
case "$jetstream" in 0|1) ;; *) printf 'SPARROW_JETSTREAM must be 0 or 1\n' >&2; exit 2;; esac
case "$mode" in candidate|release) ;; *) printf 'invalid SPARROW_BUILD_MODE\n' >&2; exit 2;; esac
if [[ "$mode" == release ]]; then
    [[ $(git rev-parse --show-toplevel) == "$root" ]] || exit 2
    git diff --quiet HEAD -- Cargo.toml Cargo.lock rust-toolchain.toml crates experiments scripts deploy .github tests README.md docs
    test -z "$(git ls-files --others --exclude-standard -- Cargo.toml Cargo.lock rust-toolchain.toml crates experiments scripts deploy .github tests README.md docs)"
fi
test ! -e "$out"
[[ $(rustc --version) == 'rustc 1.98.0 '* ]] || { printf 'Rust 1.98.0 required\n' >&2; exit 2; }
host=$(rustc -vV | sed -n 's/^host: //p')
[[ "$host" == x86_64-unknown-linux-gnu ]] || { printf 'Only Linux x86_64 is release-validated\n' >&2; exit 2; }
mkdir -p "$out/bin" "$out/evidence" "$out/deploy" "$out/docs"
out=$(cd "$out" && pwd)
for source_dir in crates experiments scripts deploy .github tests; do
    case "$out/" in "$root/$source_dir/"*) printf 'package must be outside fingerprinted source directories\n' >&2; exit 2;; esac
done
target=${CARGO_TARGET_DIR:-$root/target}
mkdir -p "$target"
target=$(cd "$target" && pwd)
for source_dir in crates experiments scripts deploy .github tests; do
    case "$target/" in "$root/$source_dir/"*) printf 'target must be outside fingerprinted source directories\n' >&2; exit 2;; esac
done
export CARGO_TARGET_DIR="$target"
export SPARROW_BUILD_COMMIT=${SPARROW_BUILD_COMMIT:-$(git rev-parse HEAD)}
# This is a source fingerprint, not a claim that a dirty build equals HEAD.
{ printf '%s\n' Cargo.toml Cargo.lock rust-toolchain.toml;
  find crates experiments scripts deploy .github tests -type f ! -name '.DS_Store' | LC_ALL=C sort; } |
    while IFS= read -r file_path; do sha256sum "$file_path"; done > "$out/evidence/source-files.sha256"
(cd "$out" && sha256sum evidence/source-files.sha256) > "$out/evidence/source-manifest.sha256"
# Do not silently ship raw local diffs (which can contain secrets). Source
# fingerprints are always included; patch inclusion is explicit and scoped.
if [[ ${SPARROW_INCLUDE_SOURCE_PATCH:-0} == 1 ]]; then
    git diff --binary HEAD -- Cargo.toml Cargo.lock rust-toolchain.toml crates experiments scripts deploy .github tests > "$out/evidence/tracked-source.patch"
fi
rustc -vV > "$out/evidence/rustc.txt"
cargo --version > "$out/evidence/cargo.txt"
cp Cargo.lock "$out/evidence/Cargo.lock"
for pair in sparrow-server:sparrow-server sparrow-cli:sparrowctl; do
    package=${pair%:*}; binary=${pair#*:}
    features=()
    if [[ "$jetstream" == 1 && "$binary" == sparrow-server ]]; then features=(--features jetstream); fi
    cargo build --locked --release --quiet --target "$host" --no-default-features -p "$package" --bin "$binary" \
        "${features[@]}" \
        > "$out/evidence/$binary-build.log" 2>&1
    cargo tree --locked --target "$host" --no-default-features -p "$package" -e normal,build \
        "${features[@]}" \
        > "$out/evidence/$binary-dependencies.txt"
    cargo tree --locked --target "$host" --no-default-features -p "$package" -e features \
        "${features[@]}" \
        > "$out/evidence/$binary-features.txt"
    if grep -Eq 'feature "demo-io"|(^|[[:space:]])(arrow|cranelift)(-[[:alnum:]_-]+)? v' "$out/evidence/$binary-features.txt"; then
        printf 'Unexpected production feature/dependency\n' >&2; exit 3
    fi
    if [[ "$binary" == sparrowctl ]] && grep -Eq 'sparrow-(runtime|testkit|connectors) v' "$out/evidence/$binary-dependencies.txt"; then
        printf 'CLI must not link the runtime/testkit/connectors\n' >&2; exit 3
    fi
    if grep -Eq 'sparrow-testkit v' "$out/evidence/$binary-dependencies.txt"; then
        printf 'Testkit must not enter production dependency graph\n' >&2; exit 3
    fi
    if grep -Eq '(tokio-websockets|openssl-sys) v' "$out/evidence/$binary-dependencies.txt"; then
        printf 'Unexpected WebSocket/native OpenSSL production dependency\n' >&2; exit 3
    fi
    if [[ "$jetstream" == 0 || "$binary" == sparrowctl ]] && grep -q 'async-nats v' "$out/evidence/$binary-dependencies.txt"; then
        printf 'NATS SDK leaked into a feature-off production binary\n' >&2; exit 3
    fi
    install -m 755 "$target/$host/release/$binary" "$out/bin/$binary"
done
cp deploy/sparrow.service deploy/production.env.example deploy/pipeline-aligned.json deploy/pipeline-k1-zero.json deploy/pipeline-k1-two-count.json "$out/deploy/"
cp docs/PRODUCTION.md "$out/docs/"
cp docs/DAG.md "$out/docs/"
cp deploy/pipeline-k3-graph.json "$out/deploy/"
cp docs/IOT.md "$out/docs/"
cp docs/REFERENCE_TABLES.md "$out/docs/"
cp deploy/reference-table-limits.json deploy/pipeline-reference-lookup.json deploy/reference-lookup.ndjson deploy/reference-lookup.expected.json "$out/deploy/"
cp deploy/pipeline-reference-lookup-aligned.json "$out/deploy/"
cp deploy/pipeline-k4-change.json deploy/pipeline-k4-deadband.json deploy/k4-change.ndjson deploy/k4-deadband.ndjson deploy/k4-change.expected.json deploy/k4-deadband.expected.json "$out/deploy/"
cp deploy/stream-k4-telemetry.json "$out/deploy/"
cp deploy/pipeline-k4-hysteresis.json deploy/k4-hysteresis.ndjson deploy/k4-hysteresis.expected.json "$out/deploy/"
cp deploy/stream-iot-timed.json deploy/pipeline-iot-hold-for.json deploy/pipeline-iot-debounce.json "$out/deploy/"
if [[ "$jetstream" == 1 ]]; then
    cp deploy/pipeline-jetstream.json deploy/pipeline-jetstream-iot.json deploy/nats-jetstream-local.conf.example "$out/deploy/"
    cp docs/JETSTREAM.md "$out/docs/"
fi
"$out/bin/sparrow-server" --version > "$out/evidence/server-version.txt"
"$out/bin/sparrowctl" --version > "$out/evidence/cli-version.txt"
jq -n --arg commit "$SPARROW_BUILD_COMMIT" --arg target "$host" --arg mode "$mode" \
    --argjson jetstream "$jetstream" \
    --arg source "$(sha256sum "$out/evidence/source-files.sha256" | cut -d' ' -f1)" \
    '{format:"sparrow-build-v1",source_commit:$commit,source_manifest_sha256:$source,
      target:$target,rust:"1.98.0",profile:"release",build_mode:$mode,default_features:false,
      jetstream_enabled:($jetstream==1),jetstream_maturity:"preview_not_profile_certified",
      binaries:["sparrow-server","sparrowctl"],certification:"requires_matching_test_evidence"}' > "$out/build.json"
(cd "$out" && find bin deploy docs evidence -type f -print | LC_ALL=C sort | while IFS= read -r file_path; do sha256sum "$file_path"; done; sha256sum build.json) > "$out/SHA256SUMS"
printf 'PRODUCTION_PACKAGE_OK %s\n' "$out"

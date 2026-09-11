#!/usr/bin/env bash
# Full-service benchmark. Mosquitto is required; no demo-session shortcuts.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${SPARROW_BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  BUILD_LOG="${SPARROW_BENCH_BUILD_LOG:-/tmp/sparrow-bench-build.log}"
  echo "Building release binaries; output: $BUILD_LOG"
  if ! cargo build --locked --release -p sparrow-cli --bin sparrow_bench -p sparrow-server --bin sparrow-server >"$BUILD_LOG" 2>&1; then
    sed -n '1,100p' "$BUILD_LOG" >&2
    exit 1
  fi
fi
exec "${CARGO_TARGET_DIR:-target}/release/sparrow_bench" "$@"

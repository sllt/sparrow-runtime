#!/usr/bin/env bash
# Host-specific end-to-end product bench. Numbers are not SLOs.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== sparrow end-to-end bench (not SLOs; not exactly-once) =="
cargo run -p sparrow-cli --bin sparrow_bench --release
echo "== bench: ok =="

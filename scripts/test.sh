#!/usr/bin/env bash
# CI-friendly M0 checks. No network services required.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== cargo test --workspace =="
cargo test --workspace --offline --locked 2>/dev/null || cargo test --workspace

echo "== G0 fixtures (explicit) =="
cargo test -p sparrow-testkit g0_corpus -- --nocapture

echo "== M0 smoke demo =="
cargo run -p sparrow-testkit --example m0_pipeline_smoke

echo "== G1a experiments =="
cargo run -p layout-rowbatch
cargo run -p arrow-evaluation

echo "all M0 checks passed"

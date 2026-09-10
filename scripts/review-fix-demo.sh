#!/usr/bin/env bash
# Process / API evidence for the static-review fix set (R01–R28 / V01–V03).
# Covers the failure/edge matrix, not only happy-path units.
# Run the entire suite once: this includes every historical prefix and R3/R4.
# Repeating cargo test --workspace for each prefix starts thousands of empty
# binary/doc-test harnesses and can spend far longer in startup than testing.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== full workspace regression suite (including R1/R2/R3/R4) =="
cargo test --workspace --locked -- --test-threads=8

echo "== HTTP API recovery + metrics (review_api integration) =="
cargo test -p sparrow-server --test review_api --locked -- --nocapture

echo "== review-fix-demo: ok =="

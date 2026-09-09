#!/usr/bin/env bash
# CI-friendly M0–M3 checks. MQTT/HTTP use in-process test servers.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== cargo test --workspace =="
cargo test --workspace --offline --locked 2>/dev/null || cargo test --workspace

echo "== G0 fixtures =="
cargo test -p sparrow-sql --lib -- --nocapture
cargo test -p sparrow-testkit g0_corpus -- --nocapture

echo "== M0 smoke =="
cargo run -p sparrow-testkit --example m0_pipeline_smoke

echo "== M1 kernel + SQL/Graph =="
cargo run -p sparrow-testkit --example m1_kernel_smoke
cargo run -p sparrow-testkit --example m1_sql_graph_equiv

echo "== M2 closed loop =="
cargo run -p sparrow-cli --bin m2_mqtt_http_loop

echo "== M3 / V0.1 server demo =="
bash scripts/m3-demo.sh

echo "== V0.2 process demos =="
bash scripts/v02-demo.sh

echo "== G1a experiments =="
cargo run -p layout-rowbatch
cargo run -p arrow-evaluation

echo "all M0–M3 / V0.1 / V0.2 checks passed"

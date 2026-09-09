# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**, V0.3.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

**Delivery is `live_best_effort` + `restart_fresh` (`recovery=none`).**

- Event-time tumbling + hopping windows, watermarks, holdback, late side output
- Processing-time tumbling windows and count windows (arrival-order; they do **not** impersonate event-time)
- Incremental COUNT/SUM/AVG/MIN/MAX (checked integer overflow)
- Versioned as-of-event-time lookup (beyond V0.2 static freeze)
- No checkpoint recovery (V0.4+)
- MQTT replay is **unsupported**
- A process restart is a **fresh attempt**, not restore
- Window results are **not** crash-identical
- Exactly-once / at-least-once configs are **rejected**

当前里程碑 / current milestone: **V0.3**（event-time + watermark + holdback + hop + versioned lookup）。

## Quick start

Requires a recent stable Rust toolchain (edition 2021). Uses the host default toolchain (no pinned rust-toolchain.toml).

```bash
export SPARROW_TOKEN=dev-token

# Control plane + in-process MQTT broker and HTTP capture
cargo run -p sparrow-server -- --token "$SPARROW_TOKEN" --demo-io --catalog /tmp/sparrow.v01.db

# Default listen: http://127.0.0.1:43180
curl -s http://127.0.0.1:43180/v1/health
```

Then create a stream and a pipeline (see `docs/m3-report.md`) and:

```bash
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" \
  -X POST http://127.0.0.1:43180/v1/pipelines/hot/start
```

`POST /start` commits **desired** state immediately. The supervisor starts
MQTT/HTTP afterwards. That is not crash recovery.

## Build & test

```bash
cargo test --workspace

# V0.1 API demo (starts a real sparrow-server, curl happy path + rejects + restart)
bash scripts/m3-demo.sh

# M2 closed loop (no control plane)
cargo run -p sparrow-cli --bin m2_mqtt_http_loop

# M1 / M0
cargo run -p sparrow-testkit --example m1_kernel_smoke
cargo run -p sparrow-testkit --example m1_sql_graph_equiv
cargo run -p sparrow-testkit --example m0_pipeline_smoke

# V0.2 process demos (virtual-clock windows, dedup, table, HTTP→MQTT)
bash scripts/v02-demo.sh
cargo run -p sparrow-cli --bin v02_pt_tumble_avg
cargo run -p sparrow-cli --bin v02_count_window
cargo run -p sparrow-cli --bin v02_bounded_dedup
cargo run -p sparrow-cli --bin v02_static_table
cargo run -p sparrow-cli --bin v02_http_mqtt_loop

# V0.3 process demos (injected event times — not wall clock)
bash scripts/v03-demo.sh
cargo run -p sparrow-cli --bin v03_et_tumble_avg
cargo run -p sparrow-cli --bin v03_hop_overlap
cargo run -p sparrow-cli --bin v03_idle_active
cargo run -p sparrow-cli --bin v03_versioned_lookup

bash scripts/test.sh
```

## V0.3 (honest)

Shipped: event-time binding, per-input watermarks (idle/active, no
backward WM), final-only lateness with output holdback
(`wm_out ≤ wm_in - L`), ET tumble + hopping (planner overlap cap),
versioned as-of-event-time lookup, SQL/Graph for that subset.

**Not shipped:** checkpoint restore, session late merge, retract,
stream-stream join, exactly-once, NATS, WASM, Graph Designer UI.
Graph/SQL remain single-source; multi-input WM is the `WatermarkHub` API
(`v03_idle_active`). Count / PT windows cannot silently use event-time.

See `docs/v03-report.md`.

`sqlparser = "=0.62.0"` is used only by `sparrow-sql`.
`sparrow-runtime` does **not** depend on HTTP, SQLite, MQTT, Axum, Arrow, or SQL crates.

## Binary

| | |
|---|---|
| Name | `sparrow-server` |
| Bind | `127.0.0.1:43180` (loopback default; `--allow-remote` to override) |
| Auth | `Authorization: Bearer <token>` (`--token` / `SPARROW_TOKEN`) |
| Catalog | `--catalog PATH` (SQLite) |
| Safe mode | `--safe-mode` — do not auto-start pipelines whose last attempt failed |
| Demo I/O | `--demo-io` — embedded MQTT + HTTP capture |

Flags: `--bind` `--token` `--catalog` `--safe-mode` `--demo-io` `--allow-remote`.

## Workspace

```
crates/sparrow-model        IDs, types, errors, RowBatch, MemoryLease, WorkBudget
crates/sparrow-expr         expression IR, eval, numeric stride kernels
crates/sparrow-plan         GraphSpec, catalog, BoundLogical, physical fusion
crates/sparrow-sql          G0 gate + SQL → same BoundLogicalPlan
crates/sparrow-io           I/O contracts (no connectors)
crates/sparrow-formats      bounded JSON codec
crates/sparrow-connectors   MQTT source/sink, HTTP push + HTTP/Log sinks
crates/sparrow-runtime      Kernel, MemoryState, watermarks, windows (no MQTT/HTTP/SQLite/Axum)
crates/sparrow-control      SQLite catalog + desired→actual supervisor
crates/sparrow-server       authenticated /v1 API + sparrow-server binary
crates/sparrow-cli          M2 composition-root demo
crates/sparrow-testkit      fixtures, virtual clock, M0/M1 demos
experiments/               G1a only
docs/                      architecture, ADRs, M0–M3 reports
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes + keys + timers)
- V0.3 delivery: `live_best_effort` + `restart_fresh` (`recovery=none` for PT/ET windows)
- MQTT replay is **unsupported**; durable recovery configs are rejected
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)
- Control plane and connectors stay **outside** `sparrow-runtime`

## Non-goals (V0.3)

Graph Designer UI, WASM, distributed execution, exactly-once, checkpoint
restore, session late merge, retract, stream-stream join, NATS,
multi-user RBAC, claimed SLOs.

See `docs/v03-report.md` and `docs/v02-report.md`.

Repository: https://github.com/sllt/sparrow-runtime

License: MIT

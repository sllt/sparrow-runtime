# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**, V0.2.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

**Delivery is `live_best_effort` + `restart_fresh` (`recovery=none`).**

- Processing-time tumbling windows and count windows
- Incremental COUNT/SUM/AVG/MIN/MAX (checked integer overflow)
- No event-time / watermark (V0.3)
- No checkpoint recovery (V0.4+)
- MQTT replay is **unsupported**
- A process restart is a **fresh attempt**, not restore
- PT window results are **not** crash-identical
- Exactly-once / at-least-once configs are **rejected**

当前里程碑 / current milestone: **V0.2**（PT/count windows + MemoryState + HTTP Push / MQTT Sink）。

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

bash scripts/test.sh
```

## V0.2 (honest)

Shipped: PT tumbling + count windows, incremental aggregates, task-owned
`MemoryState` with detach + quotas, bounded timers (generation cancel),
bounded Deduplicate (TTL + max_keys required), static ReferenceTable
snapshots (new Job = new table; running Job keeps the old Arc), HTTP Push
Source and MQTT Sink (connectors crate only).

**Not shipped:** event-time / watermark, checkpoint restore, session merge,
retract, stream-stream join, WASM, Graph Designer UI.

See `docs/v02-report.md`.

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
crates/sparrow-runtime      Kernel, MemoryState, windows (no MQTT/HTTP/SQLite/Axum)
crates/sparrow-control      SQLite catalog + desired→actual supervisor
crates/sparrow-server       authenticated /v1 API + sparrow-server binary
crates/sparrow-cli          M2 composition-root demo
crates/sparrow-testkit      fixtures, virtual clock, M0/M1 demos
experiments/               G1a only
docs/                      architecture, ADRs, M0–M3 reports
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes + keys + timers)
- V0.2 delivery: `live_best_effort` + `restart_fresh` (`recovery=none` for PT windows)
- MQTT replay is **unsupported**; durable recovery configs are rejected
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)
- Control plane and connectors stay **outside** `sparrow-runtime`

## Non-goals (V0.2)

Graph Designer UI, WASM, event-time / watermarks, distributed execution,
exactly-once, checkpoint restore, multi-user RBAC, claimed SLOs.

See `docs/v02-report.md` and `docs/m3-report.md`.

Repository: https://github.com/sllt/sparrow-runtime

License: MIT

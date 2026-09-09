# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**, V0.4.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

**Default delivery is `live_best_effort` + `restart_fresh` (`recovery=none`).**

- Event-time tumbling + hopping windows, watermarks, holdback, late side output
- Processing-time tumbling windows and count windows (arrival-order; they do **not** impersonate event-time)
- Incremental COUNT/SUM/AVG/MIN/MAX (checked integer overflow)
- Versioned as-of-event-time lookup
- **Experimental** aligned single-job checkpoint (`experimental_aligned`) for a Replayable **File** source only — **not** default exactly-once, **not** production-ready
- MQTT replay is **unsupported**; MQTT cannot pretend durable restore
- A default process restart is a **fresh attempt**, not restore
- Exactly-once / at-least-once configs are **rejected**

当前里程碑 / current milestone: **V0.4**（experimental recovery + Graph explain + connector SDK conformance）。

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

Graph validate / explain (offline-friendly; catalog may be embedded):

```bash
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" \
  -X POST http://127.0.0.1:43180/v1/graphs/explain \
  --data-binary @graph.json
```

## Build & test

```bash
cargo test --workspace

# V0.4 process demos (file checkpoint kill/restore, MQTT reject, Graph explain)
bash scripts/v04-demo.sh
cargo run -p sparrow-cli --bin v04_file_checkpoint -- --data FILE --chk DIR --mode gold
cargo run -p sparrow-cli --bin v04_mqtt_reject
cargo run -p sparrow-cli --bin v04_graph_author -- explain graph.json

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

# V0.3 process demos (injected event times — not wall clock)
bash scripts/v03-demo.sh

bash scripts/test.sh
```

## V0.4 (honest)

Shipped: File/replay test Source (message-boundary cuts, identity/rotation),
**experimental** aligned single-job checkpoint (barrier, freeze+chunk write,
manifest commit, recover from committed only, crash-cut tests), Graph
validate/explain (physical / fusion / time / state / guarantee) plus a
minimal offline authoring CLI, Connector SDK conformance
(ReplayableSource / Sink flush), capability matrix rejects.

**Experimental — not production-ready:** checkpoint durability, exactly-once,
MQTT restore, unaligned/multi-job barriers, incremental checkpoints.

**Not shipped:** WASM operator runtime (optional spike under
`experiments/wasm-spike/`, off default build), Graph Designer UI, session
late merge, retract, stream-stream join, NATS.

See `docs/v04-report.md`.

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
crates/sparrow-plan         GraphSpec, catalog, BoundLogical, physical fusion, explain
crates/sparrow-sql          G0 gate + SQL → same BoundLogicalPlan
crates/sparrow-io           I/O contracts + ReplayableSource
crates/sparrow-formats      bounded JSON codec
crates/sparrow-connectors   MQTT source/sink, HTTP, File/replay source
crates/sparrow-runtime      Kernel, MemoryState, windows, experimental checkpoint
crates/sparrow-control      SQLite catalog + desired→actual supervisor
crates/sparrow-server       authenticated /v1 API + sparrow-server binary
crates/sparrow-cli          composition-root demos
crates/sparrow-testkit      fixtures, virtual clock, M0/M1 demos
experiments/               G1a + optional WASM spike (not a workspace member)
docs/                      architecture, ADRs, milestone reports
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes + keys + timers)
- V0.4 default delivery: `live_best_effort` + `restart_fresh` (`recovery=none` for PT/ET windows)
- `experimental_aligned` is opt-in, File/replay only, **not** exactly-once
- MQTT replay is **unsupported**; durable recovery configs are rejected
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)
- Control plane and connectors stay **outside** `sparrow-runtime`

## Non-goals (V0.4)

Graph Designer UI product, production checkpoint, WASM operator runtime,
distributed execution, exactly-once, session late merge, retract,
stream-stream join, NATS, multi-user RBAC, claimed SLOs.

See `docs/v04-report.md`.

Repository: https://github.com/sllt/sparrow-runtime

License: MIT

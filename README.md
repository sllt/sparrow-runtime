# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**, V1.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

**Default delivery is `live_best_effort` + `restart_fresh` (`recovery=none`).**

- Event-time tumbling + hopping windows, watermarks, holdback, late side output
- Processing-time tumbling windows and count windows (arrival-order; they do **not** impersonate event-time)
- Incremental COUNT/SUM/AVG/MIN/MAX (checked integer overflow)
- Versioned as-of-event-time lookup (checkpoint binds table revision)
- **Production** aligned single-job checkpoint (`aligned`) for a Replayable **File** source only — **not** default exactly-once
- Recover only from verified committed checkpoints; missing/corrupt stores are rejected
- MQTT replay is **unsupported**; MQTT cannot pretend durable restore
- A default process restart is a **fresh attempt**, not restore
- Exactly-once / at-least-once configs are **rejected**

当前里程碑 / current milestone: **V1**（production aligned recovery + coordinator + observability）。

## Quick start

Requires a recent stable Rust toolchain (edition 2021). Uses the host default toolchain (no pinned rust-toolchain.toml).

```bash
export SPARROW_TOKEN=dev-token

# Control plane + in-process MQTT broker and HTTP capture
cargo run -p sparrow-server -- --token "$SPARROW_TOKEN" --demo-io --catalog /tmp/sparrow.v01.db

# Default listen: http://127.0.0.1:43180
curl -s http://127.0.0.1:43180/v1/health
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" http://127.0.0.1:43180/v1/metrics
```

Then create a stream and a pipeline (see `docs/m3-report.md`) and:

```bash
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" \
  -X POST http://127.0.0.1:43180/v1/pipelines/hot/start
```

`POST /start` commits **desired** state immediately. The supervisor starts
MQTT/HTTP afterwards. MQTT pipelines are still `restart_fresh`.

File/replay pipelines may set `"recovery":"aligned"` and restore from a
committed checkpoint via the **HTTP API** (`/start`, `/checkpoint`,
`/restore`, `/kill`). This is **not** exactly-once. MQTT + `aligned` is
still rejected.

## Build & test

```bash
cargo test --workspace

# V1 process demos (production aligned file checkpoint, MQTT reject, soak, API)
bash scripts/v1-demo.sh
bash scripts/review-fix-demo.sh
bash scripts/bench.sh
cargo run -p sparrow-cli --bin v1_file_checkpoint -- --data FILE --chk DIR --mode gold
cargo run -p sparrow-cli --bin v1_mqtt_reject
cargo run -p sparrow-cli --bin v1_soak
cargo run -p sparrow-cli --bin sparrow_bench --release

# V0.4 process demos (same File path; policy name is now aligned)
bash scripts/v04-demo.sh

# V0.1 API demo (starts a real sparrow-server, curl happy path + rejects + restart)
bash scripts/m3-demo.sh

# M2 closed loop (no control plane)
cargo run -p sparrow-cli --bin m2_mqtt_http_loop

# M1 / M0
cargo run -p sparrow-testkit --example m1_kernel_smoke
cargo run -p sparrow-testkit --example m1_sql_graph_equiv
cargo run -p sparrow-testkit --example m0_pipeline_smoke

# V0.2 / V0.3 process demos
bash scripts/v02-demo.sh
bash scripts/v03-demo.sh

bash scripts/test.sh
```

## V1 (honest)

Shipped: File/replay Source (message-boundary cuts, identity/rotation),
**production** aligned single-job checkpoint (versioned codecs,
OperatorId/StateSlotKey checks, coordinator timeout/abort/stop, recover
from committed only), Graph validate/explain, Connector SDK conformance,
capability matrix rejects, status `effective` guarantees, `/v1/metrics`.

**Not exactly-once.** MQTT restore is rejected. Session windows (L=0) and
local parallelism shards are optional/incomplete and do not block this
release.

**Not shipped:** WASM operator runtime (optional spike under
`experiments/wasm-spike/`, off default build), Graph Designer UI, session
late merge, retract, stream-stream join, NATS, distributed shuffle.

See `docs/v1-report.md`.

`sqlparser = "=0.62.0"` is used only by `sparrow-sql`.
`sparrow-runtime` does **not** depend on HTTP, SQLite, MQTT, Axum, Arrow, or SQL crates.

## Binary

| | |
|---|---|
| Name | `sparrow-server` |
| Bind | `127.0.0.1:43180` (loopback default; `--allow-remote` to override) |
| Auth | `Authorization: Bearer <token>` (`--token` / `SPARROW_TOKEN`) |
| Catalog | `--catalog PATH` (SQLite) |
| Safe mode | `--safe-mode` — do not auto-start pipelines whose last attempt failed; file paths require `SPARROW_DATA_ROOTS` |
| Demo I/O | `--demo-io` — embedded MQTT + HTTP capture |
| Data roots | `SPARROW_DATA_ROOTS` — colon-separated file/checkpoint allowlist |

Flags: `--bind` `--token` `--catalog` `--safe-mode` `--demo-io` `--allow-remote`.

**File path allowlist (N3):** `check_data_path` compares a *normalized* path (lexical `..` / `.`, then canonicalize when the prefix exists). The original path is never used as a `starts_with` fallback. CWD is never a default root (systemd cwd can be `/`). When `SPARROW_DATA_ROOTS` is unset, the only default is `{temp_dir}/sparrow` — not `/tmp` as a whole. `--safe-mode` / `SPARROW_SAFE_MODE=1` without `SPARROW_DATA_ROOTS` denies file paths. Demos should export `SPARROW_DATA_ROOTS` to their workdir.

## Workspace

```
crates/sparrow-model        IDs, types, errors, RowBatch, MemoryLease, WorkBudget
crates/sparrow-expr         expression IR, eval, numeric stride kernels
crates/sparrow-plan         GraphSpec, catalog, BoundLogical, physical fusion, explain, compat
crates/sparrow-sql          G0 gate + SQL → same BoundLogicalPlan
crates/sparrow-io           I/O contracts + ReplayableSource
crates/sparrow-formats      bounded JSON codec
crates/sparrow-connectors   MQTT source/sink, HTTP, File/replay source
crates/sparrow-runtime      Kernel, MemoryState, windows, aligned checkpoint, coordinator
crates/sparrow-control      SQLite catalog + desired→actual supervisor
crates/sparrow-server       authenticated /v1 API + sparrow-server binary
crates/sparrow-cli          composition-root demos
crates/sparrow-testkit      fixtures, virtual clock, M0/M1 demos
experiments/               G1a + optional WASM spike (not a workspace member)
docs/                      architecture, ADRs, milestone reports
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes + keys + timers)
- V1 default delivery: `live_best_effort` + `restart_fresh` (`recovery=none` for PT/ET windows)
- `aligned` is opt-in, File/replay only, **not** exactly-once
- MQTT replay is **unsupported**; durable recovery configs are rejected
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)
- Control plane and connectors stay **outside** `sparrow-runtime`

## Non-goals (V1)

Graph Designer UI product, WASM operator runtime, distributed execution,
exactly-once, session late merge, retract, stream-stream join, NATS,
multi-user RBAC, claimed SLOs.

See `docs/v1-report.md`.

Repository: https://github.com/sllt/sparrow-runtime

License: MIT

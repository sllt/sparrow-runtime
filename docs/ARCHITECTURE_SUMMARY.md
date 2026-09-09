# Sparrow architecture summary (M0)

Sparrow is a **single-node IoT/Edge streaming dataflow runtime**. It is not a
distributed Flink clone and not a Rust eKuiper clone.

## Product invariants

1. **Dataflow-first.** SQL text and Graph definitions share one typed IR,
   one validator, and one runtime semantics. `sparrow-plan` / `sparrow-expr`
   are that shared surface (thin in M0).
2. **All buffers are bounded** in bytes, rows, and a work budget. Compact and
   Performance are *budget profiles*, not two engines.
3. **Delivery is explicit.** V0.1 is `live_best_effort` + `restart_fresh`.
   Exactly-once, MQTT session resume, and checkpoint restore are rejected at
   the contract boundary (`RestoreClaim::validate`).
4. **Failure attribution is in-process** (pipeline / job attempt / operator).
   There is no process isolation in V0.1.
5. **The runtime must not depend on HTTP, SQLite, or SQL crates.**
   `sqlparser` is pinned for the G0 gate in `sparrow-testkit` only.
   Arrow / DataFusion live only under `experiments/`.

## Crate map

| Crate | Role in M0 |
|---|---|
| `sparrow-model` | IDs, `DataType`/`Schema`, structured errors, frames, delivery + resource vocabulary, provisional `RowBatch` + `MemoryLease` |
| `sparrow-expr` | Scalar expression IR + evaluator (CAST/TRY_CAST, IS NULL, limited builtins, Dynamic extract) |
| `sparrow-plan` | Linear logical plan stub (Source → Filter → Project → Sink) |
| `sparrow-io` | Decoder / Source / Sink *contracts* only |
| `sparrow-runtime` | In-process linear executor, job-level error attribution |
| `sparrow-testkit` | Fixtures, virtual clock, capture sink, G0 SQL corpus runner |
| `experiments/arrow-evaluation` | G1a RowBatch vs Arrow (optional DataFusion feature) |
| `experiments/layout-rowbatch` | G1a RowBatch-only candidate timings |

## Memory model

- **Reservation** — operator working set (builders, scratch batches).
- **Retention** — detached copies for long-lived state.
- **Queue** — in-flight batches (backpressure credits).
- **Physical bytes** are counted *once* per allocation. `share` adds a handle;
  `detach` copies and allocates again.

`RowBatch` is the **V0.1 default** (`docs/adr/003-layout-decision.md`). Arrow is experiments-only.

## Out of scope for M0 / V0.1

MQTT and HTTP connectors, SQLite catalog, Axum server, WASM, windows,
checkpoint, UI, claimed performance SLOs.

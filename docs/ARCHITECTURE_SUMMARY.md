# Sparrow architecture summary (M2)

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
5. **The runtime must not depend on HTTP, SQLite, MQTT, or SQL crates.**
   `sqlparser` lives in `sparrow-sql` only. Arrow lives under `experiments/`.
   Tokio is allowed in `sparrow-runtime` for ExecutionChain tasks (M1).

## Crate map

| Crate | Role in M2 |
|---|---|
| `sparrow-model` | IDs, types, errors, delivery/resource, `RowBatch`, `MemoryLease`, `WorkBudget` |
| `sparrow-expr` | Scalar IR + eval + numeric filter stride |
| `sparrow-plan` | GraphSpec, catalog, `BoundLogicalPlan`, physical fusion |
| `sparrow-sql` | G0 gate + SQL binder onto the same IR (not a runtime dep) |
| `sparrow-io` | Decoder / Source / Sink *contracts* only |
| `sparrow-formats` | Bounded JSON decode/encode, schema + bad-record policy |
| `sparrow-connectors` | MQTT source, HTTP/Log sinks, `SecretResolver`, `TargetPolicy` |
| `sparrow-runtime` | `Kernel`: ExecutionChain, bounded mailboxes, `live_in`/`live_out` channels |
| `sparrow-cli` | Composition root (MQTT/HTTP stay out of the runtime crate) |
| `sparrow-testkit` | Fixtures, virtual clock, M0/M1 demos |
| `experiments/*` | G1a only |

## Memory model

- **Reservation** — operator working set (builders, scratch batches).
- **Retention** — detached copies for long-lived state.
- **Queue** — in-flight batches (backpressure credits).
- **Physical bytes** are counted *once* per allocation. `share` adds a handle;
  `detach` copies and allocates again.

`RowBatch` is the **V0.1 default** (`docs/adr/003-layout-decision.md`). Arrow is experiments-only.

## Execution (M1)

`GraphSpec` JSON and SQL text both bind to `BoundLogicalPlan`. `physicalize`
emits a linear chain; adjacent Filter→Project→Map become one transform stage
(one Tokio task, no intermediate mailbox). Mailboxes are bounded in items and
bytes; send/recv select on a cancellation token so a full queue cannot
deadlock `stop()`. Two jobs do not share mailboxes, so a stalled sink on job A
does not freeze job B.

## I/O (M2)

MQTT JSON frames are decoded in `sparrow-connectors` and pushed through a
**bounded** `mpsc` into `JobRequest.live_in`. The kernel still only runs
MemorySource / Transform / CaptureSink. CaptureSink also forwards batches on
`live_out` so an HTTP or Log sink (again outside the runtime crate) can POST
or print. Target hosts are deny-by-default; TLS `skip_verify` is rejected.
MQTT declares `replay=unsupported`. Delivery is still `live_best_effort` +
`restart_fresh`.

## Out of scope until M3

SQLite catalog, full auth API, Axum control plane, WASM, windows, checkpoint,
UI, fan-in/fan-out, claimed performance SLOs. Do not put those deps in
`sparrow-runtime`.

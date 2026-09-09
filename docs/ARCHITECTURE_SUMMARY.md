# Sparrow architecture summary (V0.3)

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

| Crate | Role in V0.1 |
|---|---|
| `sparrow-model` | IDs, types, errors, delivery/resource, `RowBatch`, `MemoryLease`, `WorkBudget` |
| `sparrow-expr` | Scalar IR + eval + numeric filter stride |
| `sparrow-plan` | GraphSpec, catalog, `BoundLogicalPlan`, physical fusion |
| `sparrow-sql` | G0 gate + SQL binder onto the same IR (not a runtime dep) |
| `sparrow-io` | Decoder / Source / Sink *contracts* only |
| `sparrow-formats` | Bounded JSON decode/encode, schema + bad-record policy |
| `sparrow-connectors` | MQTT source/sink, HTTP push + HTTP/Log sinks, `SecretResolver`, `TargetPolicy` |
| `sparrow-runtime` | `Kernel`: ExecutionChain, MemoryState, PT/count/ET windows, watermarks (no MQTT/HTTP/SQLite/Axum) |
| `sparrow-control` | SQLite catalog, desired vs actual, supervisor (no Axum) |
| `sparrow-server` | Bearer-token `/v1` API; `sparrow-server` binary |
| `sparrow-cli` | M2 composition-root demo |
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

## Control plane (M3)

`sparrow-control` stores pipeline revisions and **desired** state in SQLite.
A `start` call commits and returns; the supervisor later starts kernel +
MQTT + HTTP. That split is intentional: catalog commit must not wait on the
network. Process restart resets **actual** to stopped and, if desired is
still running, opens a new attempt (`restart_fresh`). `--safe-mode` skips
pipelines whose last attempt failed.

`sparrow-server` is Axum under `/v1` with a bearer token, 64KiB bodies, and
a loopback default bind. It depends on control/runtime/connectors.
`sparrow-runtime` must not depend on the server, SQLite, or Axum.

## V0.2 (state + windows)

Stateful stages (`WindowAgg`, `Deduplicate`, `Lookup`) each own a
`MemoryState` on the **retention** ledger. Values are detached copies;
input `RowBatch` buffers are never pinned. Timers are generation-cancelled
and capped. PT windows are `recovery=none`: restart is empty, not restore.

HTTP Push Source and MQTT Sink live in `sparrow-connectors` only.

## V0.3 (event time)

Event-time columns bind on the stream. Watermarks are per-input,
idle/active, and never go backward. Output holdback is
`wm_out ≤ wm_in - L` (final-only; late events after close go to a side
output). Hopping overlap is planner-capped. Versioned lookup is
as-of-event-time. Graph/SQL stay single-source.

## Out of scope after V0.3

Graph Designer UI, WASM, checkpoint restore, distributed fan-in/fan-out,
exactly-once, session late merge, retract, stream-stream join, NATS,
claimed performance SLOs. Do not put those deps in `sparrow-runtime`.

# Sparrow M1 report

Date: 2026-09-09  
Host: `cargo test --workspace` + M1 examples on `rustc 1.88.0`

M1 is **done**. This is a real in-process kernel, not a second executor and
not a layout flip (ADR-003 still holds).

## What shipped

| PR | Deliverable |
|---|---|
| PR-05 | Versioned `GraphSpec` JSON, catalog, shared binder → `BoundLogicalPlan`, `physicalize` with Filter→Project→Map fusion |
| PR-06 | `Kernel`: one Tokio task per physical stage, item+byte mailboxes, `WorkBudget`, queue admission, cancel/join with live-task accounting |
| PR-07 | `sparrow-sql` binds G0-accepted SELECT/WHERE/CAST/Project onto the **same** IR; rejects stay `FeatureUnavailable` |

Operators: MemorySource, Filter, Project, Map (pure), CaptureSink.  
Delivery is still `live_best_effort` / `restart_fresh` only.

`sparrow-runtime` depends on Tokio + workspace crates only. No Axum, SQLite,
MQTT, Arrow, or `sqlparser`.

Numeric `col ▷ lit` filters use a tight RowBatch stride in `sparrow-expr::kernels`
(same layout, not a second engine).

## How to run

```bash
cargo test --workspace

# Kernel: fixtures → fused filter/project → capture → graceful stop
cargo run -p sparrow-testkit --example m1_kernel_smoke

# SQL text and GraphSpec → identical capture rows
cargo run -p sparrow-testkit --example m1_sql_graph_equiv

# M0 sync path still works
cargo run -p sparrow-testkit --example m0_pipeline_smoke

bash scripts/test.sh
```

## What passed (G2)

| Evidence | Result |
|---|---|
| Fused vs unfused same rows | `g2_tests::fused_and_unfused_same_rows` |
| Full mailbox + stop does not deadlock | `g2_tests::full_queue_stop_does_not_deadlock` (mailbox depth 1, stalled sink, `stop` joins, `live_tasks == 0`) |
| Stop leaves no orphan tasks | smoke demo + G2 tests assert `live_tasks_after == 0` |
| Two jobs, one sink stalled | `g2_tests::stalled_job_does_not_freeze_peer` — peer finishes 2 rows |
| SQL ≡ Graph | `m1_sql_graph_equiv` prints the same 3 hot sensor rows |
| G0 corpus | still 25 / 25 via `sparrow-sql` |

Demo capture (both front-ends):

```
edge-a | 26.2 | ts:1700000001000000
edge-b | 31   | ts:1700000002000000
edge-c | 29.4 | ts:1700000004000000
```

## Known gaps before M2

1. **Linear only.** Fan-in / fan-out / windows are `FeatureUnavailable`. No checkpoint, no MQTT/HTTP connectors, no catalog store.
2. **SQL binder is V0.1-narrow.** No JOIN, ORDER BY, LIMIT, DISTINCT, TUMBLE. That is intentional (ADR-002).
3. **Ingested-row accounting** is source-stage only; there is no per-operator metrics table yet.
4. **Fairness** is “independent jobs on a shared multi-thread runtime”, not weighted scheduling.
5. **Numeric stride** covers simple compares only; general expressions still walk `eval`.
6. Do **not** claim SLOs. Do **not** flip the default layout to Arrow.

M2 should add production connectors and a durable catalog *outside* the runtime crate, not a second engine.

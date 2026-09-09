# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

当前里程碑 / current milestone: **M1**（进程内核循环、有界 mailbox、Graph+SQL 同一 IR）。

MQTT / HTTP / UI / SQLite / windows / checkpoint remain out of scope until M2.

## Build & test

Requires Rust 1.88+ (edition 2021).

```bash
cargo test --workspace

# M1 kernel demo
cargo run -p sparrow-testkit --example m1_kernel_smoke

# SQL and GraphSpec → identical capture
cargo run -p sparrow-testkit --example m1_sql_graph_equiv

# M0 sync smoke (still valid)
cargo run -p sparrow-testkit --example m0_pipeline_smoke

# G0 SQL corpus
cargo test -p sparrow-sql --lib
cargo test -p sparrow-testkit g0_corpus -- --nocapture

# G1a layout experiment (Arrow only here)
cargo run -p arrow-evaluation --release

bash scripts/test.sh
```

`sqlparser = "=0.62.0"` is pinned and used only by `sparrow-sql`.
`sparrow-runtime` does **not** depend on HTTP, SQLite, MQTT, Arrow, or SQL crates.
Tokio is used for ExecutionChain tasks (M1).

## Workspace

```
crates/sparrow-model      IDs, types, errors, RowBatch, MemoryLease, WorkBudget
crates/sparrow-expr       expression IR, eval, numeric stride kernels
crates/sparrow-plan       GraphSpec, catalog, BoundLogical, physical fusion
crates/sparrow-sql        G0 gate + SQL → same BoundLogicalPlan
crates/sparrow-io         I/O contracts (no connectors)
crates/sparrow-runtime    Kernel / mailboxes / supervisor / LinearExecutor
crates/sparrow-testkit    fixtures, virtual clock, demos
experiments/              G1a only
docs/                     architecture, ADRs, M0/M1 reports
tests/fixtures/           G0 SQL, GraphSpec JSON, records
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes)
- V0.1 delivery: `live_best_effort` + `restart_fresh` only
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)

License: MIT

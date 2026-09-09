# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

当前里程碑 / current milestone: **M2**（MQTT JSON → 共享内核 → HTTP/Log，有界缓冲，`live_best_effort`）。

## Build & test

Requires Rust 1.88+ (edition 2021).

```bash
cargo test --workspace

# M2 live closed loop (embedded MQTT broker + HTTP capture)
cargo run -p sparrow-cli --bin m2_mqtt_http_loop

# M1 kernel demo
cargo run -p sparrow-testkit --example m1_kernel_smoke

# SQL and GraphSpec → identical capture
cargo run -p sparrow-testkit --example m1_sql_graph_equiv

# M0 sync smoke (still valid)
cargo run -p sparrow-testkit --example m0_pipeline_smoke

bash scripts/test.sh
```

`sqlparser = "=0.62.0"` is pinned and used only by `sparrow-sql`.
`sparrow-runtime` does **not** depend on HTTP, SQLite, MQTT, Arrow, or SQL crates.
MQTT/HTTP live in `sparrow-connectors` and are wired by `sparrow-cli`.

## Workspace

```
crates/sparrow-model       IDs, types, errors, RowBatch, MemoryLease, WorkBudget
crates/sparrow-expr        expression IR, eval, numeric stride kernels
crates/sparrow-plan        GraphSpec, catalog, BoundLogical, physical fusion
crates/sparrow-sql         G0 gate + SQL → same BoundLogicalPlan
crates/sparrow-io          I/O contracts (no connectors)
crates/sparrow-formats     bounded JSON codec
crates/sparrow-connectors  MQTT source, HTTP/Log sinks, secrets, target policy
crates/sparrow-runtime     Kernel / mailboxes / supervisor (no MQTT/HTTP deps)
crates/sparrow-cli         composition root + M2 demo
crates/sparrow-testkit     fixtures, virtual clock, M0/M1 demos
experiments/              G1a only
docs/                     architecture, ADRs, M0/M1/M2 reports
tests/fixtures/            G0 SQL, GraphSpec JSON, records
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes)
- V0.1 delivery: `live_best_effort` + `restart_fresh` only
- MQTT replay is **unsupported**; durable recovery configs are rejected
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)

See `docs/m2-report.md` for the capability matrix and delivery honesty.

License: MIT

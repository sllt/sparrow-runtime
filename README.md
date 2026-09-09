# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

当前里程碑 / current milestone: **M0**（契约、所有权原型、G0 SQL 语料、G1a 布局实验）。

This launch is **M0 only** (PR-01 → PR-03). MQTT / HTTP / UI / SQLite are out of scope.

## Build & test

Requires Rust 1.88+ (edition 2021). The pin exists because `sqlparser 0.62` pulls crates that need that compiler.

```bash
# 全工作区测试 / workspace tests
cargo test --workspace

# G0 SQL accept/reject corpus
cargo test -p sparrow-testkit g0_corpus -- --nocapture

# M0 端到端 demo：有限 fixture → filter/project → capture
cargo run -p sparrow-testkit --example m0_pipeline_smoke

# G1a 布局对比（Arrow 仅在 experiments）
cargo run -p arrow-evaluation
cargo run -p layout-rowbatch

# 一条 CI 脚本
bash scripts/test.sh
```

`sqlparser = "=0.62.0"` is pinned in the workspace `Cargo.toml` and used only
by `sparrow-testkit`. The runtime crates do **not** depend on HTTP, SQLite, or
SQL parser crates. Arrow / DataFusion are experiments-only
(`cargo run -p arrow-evaluation --features datafusion` is optional and heavy).

## Workspace

```
crates/sparrow-model      IDs, types, errors, delivery/resource, provisional RowBatch
crates/sparrow-expr       expression IR + scalar eval
crates/sparrow-plan       linear plan stub
crates/sparrow-io         I/O contracts (no connectors)
crates/sparrow-runtime    in-process linear executor
crates/sparrow-testkit    fixtures, virtual clock, capture, G0 gate
experiments/              G1a only — not default runtime deps
docs/                     architecture, ADRs, M0 report
tests/fixtures/           G0 SQL corpus + record fixtures
```

## Invariants

- All buffers bounded (bytes + rows + work budget)
- V0.1 delivery: `live_best_effort` + `restart_fresh` only
- Job-level failure attribution in-process
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default layout (ADR-003). Arrow stays in `experiments/` — see `docs/m0-report.md`

License: MIT

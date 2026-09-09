# Sparrow M0 report

Date: 2026-09-09  
Host: cloud agent VM, `rustc 1.88.0`, `x86_64-unknown-linux-gnu`  
Method: `cargo run -p arrow-evaluation --release` and `cargo test --workspace`

These numbers are **selection-gate measurements**, not product SLOs. Do not quote them as proven latency targets.

## What M0 shipped

- PR-01: IDs, `DataType`/`Schema`, structured errors, source-frame/codec bounds, delivery + resource vocabulary, testkit (virtual clock, finite fixtures, capture sink).
- PR-02: provisional `RowBatch` + `Scalar`, `MemoryOwner`/`MemoryLease` (`acquire` / `share` / `detach` / `release`), separate reservation / retention / queue credits, bounded builder.
- PR-03: G0 SQL corpus (25 accept + 25 reject) with pinned `sqlparser = "=0.62.0"`; G1a RowBatch vs Arrow microbench; ADRs 002 and 003.

## G0

Dialect: `sqlparser::dialect::GenericDialect`.  
Runner: `cargo test -p sparrow-testkit g0_corpus`.

| bucket | count | result |
|---|---:|---|
| `tests/fixtures/sql/g0/accept` | 25 | all accepted |
| `tests/fixtures/sql/g0/reject` | 25 | all rejected |

Rejects include unbounded `ORDER BY`, any `JOIN`, window/`TUMBLE`/`HOP`/`SESSION`, unknown functions, Dynamic arithmetic without `CAST`, DDL/DML, CTE, `UNION`, `GROUP BY`, `DISTINCT`, `LIMIT`. See ADR-002.

## G1a measurements (release)

Same schema (`id`, `temp`, `name`, `payload`) and the same filter (`temp > 25`) / project / extract / fan-out shapes.

| scenario | RowBatch ns/op | Arrow ns/op | Arrow / RowBatch |
|---|---:|---:|---:|
| single-event filter/project | 73.1 | 434.8 | **5.95** (Arrow slower) |
| small-batch 128 numeric filter/project | 6619.1 | 645.7 | 0.10 (Arrow faster) |
| json/dynamic extract (64 rows) | 3223.1 | 2348.1 | 0.73 (Arrow faster) |
| fan-out 32 rules × 32 rows (share) | 39895.5 | 3992.2 | 0.10 (Arrow faster) |

Debug-profile numbers (same binary, unoptimized) showed the same *shape*: RowBatch wins single-event (~5×), Arrow wins 128-row numeric and fan-out.

`layout-rowbatch` (release): 1000 × 64-row numeric filter, 2.32 ms total, builder peak 2560 B, physical 2560 B — bounds held.

DataFusion: documented snippet only (`experiments/arrow-evaluation/src/datafusion_snippet.rs`). It is **not** a Cargo dependency. G1a compares kernels/layout, not a SQL optimizer.

### How to read the table

- Arrow kernels are vectorized; the RowBatch path is scalar `eval` per cell. That explains the 128-row and fan-out gap more than the *row vs column memory layout* itself.
- The JSON scenario is not a perfect isolator: RowBatch extracts a typed `DynamicValue` key; Arrow scans a UTF-8 JSON fragment. Treat it as “edge payload extract cost”, not a codec bake-off.
- Single-event is the first G1a scenario because Sparrow is an IoT/Edge runtime, not a warehouse engine.

## ADR-003 recommendation

**Keep provisional `RowBatch` as the V0.1 default layout. Do not switch `sparrow-runtime` / `sparrow-model` to Arrow.**

Why:

1. **Product fit.** Single-event latency is ~6× better on RowBatch in this suite. Edge jobs will spend most of their life at 1–16 rows, not 128+.
2. **Ownership is already expressed.** Reservation / retention / queue + `share`/`detach` sit on `RowBatch`. Switching layout now would redo PR-02 for no M1 blocker.
3. **Dependency budget.** Arrow 54 pulls a large crate graph. The invariant is that the *runtime* does not take SQL/HTTP/SQLite; adding Arrow as a default dep is the same class of weight and is unnecessary for M1.
4. **The 128-row gap is a kernel problem.** M1 can add a compact numeric stride (or batch the existing scalar eval) without adopting Arrow as the store format. Arrow can stay an interchange / experiment.
5. **`Dynamic` is first-class.** A JSON-like payload is normal on the edge. Forcing it through Arrow `StringArray` + parse, or a union array, is extra surface we do not need in V0.1.

What this does **not** claim:

- RowBatch is faster in general.
- Compact/Performance SLOs are proven.
- Arrow is banned forever. A later ADR may add an optional Arrow *view* for the Performance budget when `batch_rows` is large.

## Ownership tests (PR-02)

All green:

- lease credits return on drop
- `share` increments handles, physical bytes counted once
- `detach` creates a new physical alloc (typically retention)
- builder expansion fails at the byte/row cap; peak is recorded, not silent growth
- randomized acquire/share/detach/drop sequences return to zero

## Demo

```bash
cargo run -p sparrow-testkit --example m0_pipeline_smoke
```

Feeds 6 finite sensor fixtures, filters `temperature > 25`, projects `device_id`, `temperature`, `payload.temp`, `ts`, prints 3 captured rows, asserts leases return to 0.

## Open items for M1

- Bind G0-accepted SQL to `sparrow-expr` / `sparrow-plan` (narrow binder is AST-only today).
- Graph front-end emitting the same IR.
- Real connectors still out of scope unless M1 explicitly adds a finite file/MQTT *client* — do not add a server.
- Optional numeric stride inside `RowBatch` to close the 128-row gap without an Arrow default.
- Catalog / revision store — not SQLite in the runtime crate.

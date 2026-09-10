# ADR-003: Default in-memory layout for V0.1

- Status: **Accepted**
- Date: 2026-09-09
- Supersedes: provisional `RowBatch` warning in `sparrow-model`

## Context

M0 shipped a provisional row-oriented `RowBatch` plus `MemoryLease` ownership.
G1a compared that path to Arrow array kernels on four IoT/Edge-relevant
scenarios (single-event, 128-row numeric filter/project, JSON/Dynamic extract,
32-rule fan-out). DataFusion was documented as an experiments-only snippet and
was not added as a Cargo dependency.

The original selection-gate measurements are retained below. They are
historical layout/kernel experiments, not current performance SLOs.

### Selection-gate evidence

Measured on 2026-09-09 in the cloud agent VM with `rustc 1.88.0`,
`x86_64-unknown-linux-gnu`, using
`cargo run -p arrow-evaluation --release`. Both paths used the same
`id`, `temp`, `name`, `payload` schema and `temp > 25` predicate.

| Scenario | RowBatch ns/op | Arrow ns/op | Arrow / RowBatch |
| --- | ---: | ---: | ---: |
| Single-event filter/project | 73.1 | 434.8 | 5.95 |
| 128-row numeric filter/project | 6619.1 | 645.7 | 0.10 |
| JSON/Dynamic extract, 64 rows | 3223.1 | 2348.1 | 0.73 |
| Fan-out, 32 rules × 32 rows | 39895.5 | 3992.2 | 0.10 |

`layout-rowbatch --release`: 1000 × 64-row numeric filters took 2.32 ms;
builder peak and tracked physical size were both 2560 B. Arrow's vectorized
kernels versus scalar RowBatch evaluation explain much of the batch gap.
The JSON cases perform different extraction strategies, so they are not a
controlled codec comparison. DataFusion was a documented snippet only,
not a dependency or optimizer benchmark.

## Decision

**Keep `RowBatch` as the V0.1 default layout.**

Do **not** make Arrow (or DataFusion) a dependency of `sparrow-model` or
`sparrow-runtime`. Arrow remains under `experiments/arrow-evaluation`.

Rationale (see the selection-gate evidence above):

- Single-event filter/project is ~6× faster on RowBatch in the release suite.
  That path is the product's common case.
- Arrow wins at 128-row numeric and multi-rule fan-out because the current
  RowBatch kernels are scalar, not because the *lease/row* model is wrong.
- Switching now would discard the PR-02 ownership prototype and pull a large
  crate graph into the runtime.
- `Dynamic` stays a first-class scalar; we do not encode edge payloads as
  Arrow JSON strings by default.

## Consequences

- M1 kernels continue to target `sparrow_model::Scalar` / `RowBatch`.
- A later milestone may add a *numeric stride* or an optional Arrow view for
  the Performance budget when `batch_rows` is large. That is a new ADR.
- Layout is no longer "unfrozen", but it is also not a public ABI. Internal
  field order / packing may still change without a user-visible contract.
- Compact vs Performance remain budget profiles of the same engine.

## Alternatives considered

1. **Switch default to Arrow for V0.1.** Rejected: regresses single-event
   latency, violates the "experiments-only" dependency fence, and forces a
   `Dynamic` encoding decision we do not need yet.
2. **Dual default (RowBatch + Arrow).** Rejected: two engines. Compact and
   Performance are budgets, not layouts.

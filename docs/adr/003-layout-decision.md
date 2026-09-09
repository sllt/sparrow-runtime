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

Raw numbers live in `docs/m0-report.md`. They are selection-gate measurements,
not SLOs.

## Decision

**Keep `RowBatch` as the V0.1 default layout.**

Do **not** make Arrow (or DataFusion) a dependency of `sparrow-model` or
`sparrow-runtime`. Arrow remains under `experiments/arrow-evaluation`.

Rationale (see the report for the table):

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

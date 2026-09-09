# ADR-002: Narrow Sparrow SQL v0 binder

- Status: Accepted (M0 / G0)
- Date: 2026-09-09

## Context

SQL and Graph must share one typed IR. Shipping a wide SQL surface before the
layout (G1a) and runtime (M1) are frozen would invent semantics we cannot
execute (unbounded `ORDER BY`, `JOIN`, windows / `TUMBLE`).

## Decision

G0 is a **narrow binder**:

1. Parse with pinned `sqlparser = "=0.62.0"` and `GenericDialect`.
2. Walk the AST and **accept only** the V0.1 allow-list:
   simple `SELECT`, `WHERE`, `CAST` / `TRY_CAST`, aliases, `IS NULL`,
   and a closed builtin placeholder set (`abs`, `coalesce`, `lower`,
   `upper`, `length`, …).
3. **Reject** (do not silently degrade) unbounded `ORDER BY`, any `JOIN`,
   window functions, `TUMBLE` / `HOP` / `SESSION`, unknown functions,
   Dynamic arithmetic without `CAST`, DDL/DML, CTE, `UNION`, `GROUP BY`,
   `DISTINCT`, `LIMIT`, and subqueries.
4. Keep `sqlparser` out of `sparrow-runtime` and `sparrow-model`.
   The corpus lives under `tests/fixtures/sql/g0/` and is executed by
   `sparrow-testkit`.

A later binder may rewrite accepted SQL into the same `sparrow-expr` /
`sparrow-plan` IR that Graph will emit. Rejected constructs become
`FeatureUnavailable` with context, not a second dialect.

## Consequences

- The accept/reject corpus is the contract; adding syntax is an ADR, not a
  drive-by parser change.
- Users writing SQL that looks like Flink/eKuiper window SQL get a clear
  reject in M0 rather than a half-working plan.
- Graph front-end work in M1 does not wait on a full SQL optimizer.

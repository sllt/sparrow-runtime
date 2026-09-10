# Review R2 — batch 9 (P2-30, P2-32, P2-36 leftovers)

N1–N16, batch 7, and batch 8 (P1-14/20/17/27/A2) are already merged.
Do **not** reopen them.

This batch is the optional P2 leftovers skipped in batch 8.

## P2-30 — column index at bind time

`Expr::Column` still carries a name (plan IR / SQL / Graph). [`bind`](../crates/sparrow-expr/src/bind.rs)
resolves each name to a schema index once. [`eval_bound`](../crates/sparrow-expr/src/bind.rs)
indexes the row and never scans field names.

Hot paths that used to call `eval` (and therefore `index_of_name`) per row:

- `filter_mask` binds once per batch
- `apply_steps` Project/Map binds once per step
- `LinearExecutor::from_plan` binds filter/project against the source schema
- `WindowOperator::new` binds agg input exprs

`eval(expr, schema, row)` still exists and binds then evaluates (tests / one-offs).

Tests: `p2_30_bind_resolves_column_index`,
`p2_30_eval_bound_matches_eval_without_schema`,
`p2_30_bind_unknown_column_fails`.

## P2-32 — dedup expire is a prefix pop

`DedupOperator` keeps `BTreeMap<(expire_at, encoded_key), StateKey>` with
`expire_at = last_seen + ttl`. Each `on_batch` pops only the due prefix.
A no-op tick probes the live min once. A tick that expires *k* of *n* keys
probes *k + 1*, not *n*.

Admit of an already-expired leftover unindexes then removes before put.
`cleanup` clears the heap.

Tests: `p2_32_expire_is_prefix_not_full_scan` (64 keys, 3 due → 4 probes),
`p2_32_within_ttl_still_dedups`,
`p2_32_expire_then_admit_same_key`.

## P2-36 — reference table crc32

`ReferenceTable::snapshot` stores a CRC-32 of name, version, key fields, and
sorted encoded rows (same `checkpoint::crc32` as manifests). `verify()`
recomputes and fails closed (`CodecViolation`) on mismatch.

Checked at:

- `LookupOperator` construction (static + latest versioned)
- `VersionedReferenceTable::publish`

This is an in-memory integrity check, not a snapshot-format change
(`SNAPSHOT_VERSION` stays 1). Checkpoint manifests already checksum chunks.

Tests: `p2_36_snapshot_checksum_verifies`,
`p2_36_wrong_stored_checksum_fails_closed`,
`p2_36_corrupted_row_fails_closed`,
`p2_36_publish_rejects_corrupt_table`.

## P1-14 residual (not changed)

Kernel aligned ACK still materializes `WindowFreeze` (`try_freeze`) so the
supervisor can `CheckpointSnapshot { window: acks.freeze, … }` then encode.
Putting encoded bytes on `AlignedAck` would retouch barrier pairing,
supervisor commit, and restore tests. Not cheap; left as documented
residual. Production `AlignedSession` still uses `encode_freeze_into`.

## Honest skips (P2-31/34/35/37–39/41)

These IDs are **not** marked open in `REVIEW_R1_CHECKLIST.md` or any
`REVIEW_R2_BATCH*.md` residual table (batch 7 only named the range;
batch 8 listed 30/32/33/36). No review text to implement against.

| ID | Status |
|---|---|
| P2-33 | Already done in batch 8 (`peek_deadline` heap min). |
| P2-31, P2-34, P2-35, P2-37, P2-38, P2-39, P2-41 | Skipped — unspecified. |
| Call-name `to_ascii_lowercase` per `eval_bound` | Left; not a column lookup. |
| Schema `index_of_name` HashMap | Bind-time only now; not worth a layout change. |

## Tests

Named `p2_30_`, `p2_32_`, `p2_36_` plus
`cargo test --workspace --locked` and `scripts/v1-demo.sh`.

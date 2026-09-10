# Review R2 — batch 4 (N6)

P1 leftover promoted to correctness. N1–N5 and N7 are already merged.
N8–N16 are **not** in this batch.

## N6 — freeze encode/decode cap must match `max_state_keys`

`decode_freeze` used a hard-coded `MAX_FREEZE_ENTRIES = 4096`. Official
budgets disagree:

| Profile | `max_state_keys` |
|---|---|
| compact (`host_kernel`) | 1024 |
| *(old freeze decode cap)* | **4096** |
| performance | 16384 |

`WindowOperator` / `MemoryState` honor the job budget, and `freeze()`
clones every live key. With a performance (or any custom) budget above
4096, `freeze()`+`commit()` published CURRENT, then `decode_freeze`
rejected the snapshot. The checkpoint existed and never recovered —
silent data loss. `host_kernel()` stays on compact, so production
matched by accident; choosing the performance profile was enough to
create an unrecoverable store.

Fix (both sides):

- Encode fails closed if `entries.len() > max_state_keys`. `commit`
  never writes CURRENT for a freeze the matching decode path cannot
  load.
- Decode uses the same bound (job/process `max_state_keys` on the
  store), not a hard-coded 4096 below the official performance budget.
- Codec default / untrusted-alloc ceiling is
  `MAX_FREEZE_ENTRIES = ResourceBudget::performance().max_state_keys`
  (16384) when no job bound is supplied. Snapshot bytes and
  `SNAPSHOT_VERSION` are unchanged.

`CheckpointStore::open_with_max_state_keys` and
`CheckpointSnapshot::{encode,decode}_with_max_state_keys` take the job
bound. `AlignedSession` and the supervisor aligned file path set the
store cap from the kernel/session budget.

## Tests

- `n6_performance_budget_freeze_commit_recover_roundtrip` — budget
  `max_state_keys = 8192`; ingest 5000 distinct keys (above the old
  4096 cap); freeze → commit → recover → `restore_freeze` keeps 5000
  keys.
- `n6_encode_over_max_state_keys_does_not_publish_current` — 17 entries
  against a store bound of 16 fails at encode/commit; no CURRENT.
- `n6_decode_honors_job_max_state_keys` — 5000-entry payload decodes
  with 8192 and the codec default; the old 4096 cap still rejects.
- `n6_freeze_entry_cap_matches_performance_budget`
- `n6_aligned_store_uses_budget_max_state_keys`

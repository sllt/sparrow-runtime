# Review R2 — batch 8 (P1-14, P1-20, A2 leftover, P1-17, P1-27)

N1–N16 and batch 7 are already merged. Do **not** reopen them.

Correctness / honesty first, then a bounded peek-O(1) timer leftover.

## P1-14 — freeze encode is incremental (no silent 3×)

`WindowOperator::encode_freeze_into` writes the freeze body from the live
store: it sorts **key refs** only and encodes each entry in place. Peak
on the aligned session path is **live retention + the output buffer**.
There is no `Vec<FrozenEntry>` clone of every acc/key before encode.

`CheckpointSnapshot::encode_from_operator` + `CheckpointStore::commit_encoded`
are the publish path used by `AlignedSession::checkpoint_barrier`. Encode
fails closed (no CURRENT) when:

- entry count > job `max_state_keys`
- estimated freeze body > `MAX_SNAPSHOT_BYTES` (8 MiB)
- live + estimated encoded would exceed `retention + reservation`

`encode_freeze` (legacy `WindowFreeze` value) applies the same entry/byte
caps before writing.

### Residual

The Kernel aligned ACK still materializes `WindowFreeze` (`try_freeze`)
so the supervisor can pair freeze + flush. That path can still hold
live + freeze Vec + encoded bytes. Production `AlignedSession` does not.
A later change can put encoded bytes on `AlignedAck` and drop the clone.

Tests: `p1_14_incremental_encode_matches_freeze_bytes`,
`p1_14_encode_refuses_oversized_freeze_before_current`.

## P1-20 — process-wide MemoryOwner + queue admit

`Kernel` holds one `process_owner: MemoryOwner` and a `queue_reserved`
ledger. Every job clones that owner. Admit refuses when:

- this job's mailbox worst-case exceeds the job/process `queue_bytes`
- **live reserved mailbox bytes + this job** would exceed `queue_bytes`
- **live retained + live queue + this job's mailbox** would exceed
  `retention_bytes + queue_bytes`

N jobs no longer each receive an independent full compact budget.
Queue reservations are released when the job task ends (`QueueAdmit`).

Tests: `p1_20_second_job_refused_when_process_queue_reserved`,
`p1_20_jobs_share_process_memory_owner`.

## A2 leftover — transform + tracked scalars (no arena rewrite)

`apply_steps` builds each Filter/Project/Map step into a
`RowBatchBuilder` (P1-15 acquire-before-growth). It does not
`to_vec()` the whole batch onto an unaccounted shadow `Vec<Row>`.

`Scalar::utf8` / `Scalar::bytes` stay for literals and tests but are
documented as untracked. Runtime/connector ingress should use
`Scalar::utf8_tracked(owner, …)` / `bytes_tracked`, which
`ensure_headroom` on reservation before allocating. `JsonCodec` takes
an optional owner and uses the tracked path when set.

Not in this batch: a real scalar arena / lease on every `Arc<str>`.

Tests: `a2_transform_builder_charges_owner_not_shadow_vec`,
`a2_utf8_tracked_used_on_runtime_path`,
`a2_utf8_tracked_refuses_without_reservation_headroom`,
`a2_ensure_headroom_does_not_charge`.

## P1-17 — fail-on-decode

`PipelineSpec.fail_on_decode` (default false) or
`SPARROW_FAIL_ON_DECODE=1|true|yes` makes a decode error **fail the
job**:

- File: `apply_file_poll` returns `CodecViolation` after incrementing
  `IoDiagnostics.decode_errors`, then the source cancels the kernel job.
- MQTT: `JsonCodec` uses `BadRecordPolicy::FailJob`; `MqttSource::run`
  does not reconnect on that class of error.

Default remains count-and-continue (`n5_restart_fresh_decode_errors_counted`).

Test: `p1_17_fail_on_decode_fails_the_job`.

## P1-27 — MQTT inbox vs queue budget

`inbox_capacity × json_limits.max_bytes` is still capped at 4 MiB, and
now also hard-rejected when it exceeds the process/job `queue_bytes`
(`ResourceBudget::compact().queue_bytes` = 2 MiB by default).
`check_inbox_budget` is shared by `validate` and the supervisor bind.

Tests: `p1_27_inbox_times_max_record_rejects_over_queue_budget`,
`p1_27_inbox_within_compact_queue_is_ok`.

Residual: the inbox is not a `MemoryLease` on the process owner (hard
reject only). Charging the worst-case onto the queue ledger would
double-count with mailbox admit on compact MQTT (default 32 × 64 KiB =
2 MiB).

## Optional P2 (this PR)

| ID | Status |
|---|---|
| P2-33 | Done — `BoundedTimers::peek_deadline` is O(1) heap peek. Stale gens may wake early; `fire_due` still skips them. `p2_33_peek_deadline_is_heap_min`. |
| P2-30 | Skipped — column index at bind. |
| P2-32 | Skipped — dedup still full-scans on expire. |
| P2-36 | Skipped — reference table crc32 (`checkpoint::crc32` already exists for manifests). |

## Tests

Named `p1_14_`, `p1_20_`, `p1_17_`, `p1_27_`, `a2_` plus
`cargo test --workspace` and the existing demo scripts.

# Review R2 — batch 5 (N8–N10)

P1 leftovers promoted to correctness. N1–N7 are already merged.
N11–N16 are **not** in this batch.

## N8 — converge backoff must not sleep in the shared loop

`converge_once` used to `sleep(40ms << min(consecutive_failures, 6))`
(up to 2.56s) **inside** the serial desired-state loop. One failed
pipeline stalled every healthy start/stop in the same tick.

Fix:

- Per-pipeline `next_retry_at`. Before the due instant, `continue`
  without sleeping.
- After a start/reap failure, schedule `now + retry_backoff(cf)`.
- `request_start` / `running` (consecutive_failures = 0) clears the
  slot so a manual start is not deferred.
- The 200ms `run_loop` poll is unchanged; it is not per-pipeline
  backoff.

Test: `n8_failed_pipeline_backoff_does_not_block_healthy_converge`
(sparrow-control).

## N9 — future-timestamp drops are observable

`on_et_row` returned an empty emission for `event_time > now + D`.
No lates, no counter — blueprint requires visible drops. Watermark
behavior is unchanged (P0-5: do not poison `max_et`).

`now` is **processing time** from the kernel clock (host wall, or a
virtual clock in tests), in microseconds. It is not event time and
does not advance the watermark. See `WindowOperator::on_batch` and
`EventTimeBinding::max_future_skew_micros`.

Fix:

- `WindowEmission.future_dropped` increments on each skew reject.
- Kernel `RuntimeMetrics.future_dropped` (exported on `GET /v1/metrics`).

Test: `n9_future_drop_visible_in_metrics` (sparrow-runtime).

## N10 — NaN in compare must not fail the job

`cmp_scalars` / `cmp_ord` returned `Err` on Float64 NaN, which failed
the stage and the job. SQL-ish / IEEE:

| Op | NaN |
|---|---|
| `=` | false (IEEE; NaN equals nothing) |
| `<>` | true |
| `<` `<=` `>` `>=` | false (unknown), not an error |
| `WHERE` | unknown/false → row dropped |
| `MIN` / `MAX` / `least` / `greatest` | skip NaN like NULL |

Type mismatches still fail closed.

Test: `n10_nan_compare_does_not_fail_job` (sparrow-runtime).

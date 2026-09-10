# Review R2 — batch 3 (N5 + restart_fresh EOF / decode)

P1 leftovers promoted to correctness. N1–N4 and N7 are already merged.
N6 and N8–N16 are **not** in this batch.

## N5 — EOF must not poison AppendOnly with `i64::MAX/4`

Aligned file jobs set `FileContract::AppendOnly` ("files keep growing")
but the source loop used to send `Watermark { wm_micros: i64::MAX / 4 }`
on the first EOF. Later appended rows with normal event times were all
late — silent data loss. COUNT_WINDOW tests hid this because they emit
on count, not watermark.

`FileContract` now distinguishes:

| Contract | Identity after a cut | EOF |
|---|---|---|
| `append_only` | Prefix `[0, cut)` must stay; file may grow | Sleep / poll only. **No** terminal watermark. |
| `sealed` | Same as immutable (size + fingerprint) | End of stream: one terminal watermark, then the source finishes so the job can complete and emit final ET windows. |
| `immutable` | Size + fingerprint must match | Same EOF as `sealed` (finite fixture equivalent). |

Defaults:

- Unspecified `source.file_contract` is `append_only` for both aligned and
  restart_fresh (growing files; poll on EOF; job stays up). This matches
  the historical restart_fresh file loop and does not auto-complete a
  live job just because the current file ended.
- Finite fixtures that need last ET windows set
  `source.file_contract`: `sealed` or `immutable`.

Aligned and restart_fresh share `FileReplaySource::poll_decoded` plus
`file_source::{take_file_poll, apply_file_poll, run_file_source}`.

`WindowOperator::on_batch` now keeps `pending_close` from per-row
`observe_event` so a later event time can close earlier ET windows
without a terminal MAX watermark (required for AppendOnly).

## P1-17 / P1-23 (restart_fresh half)

- Restart-fresh file decode no longer does `if let Ok(Some(row))` and
  drop the rest. `FilePoll::DecodeError` increments
  `IoDiagnostics.decode_errors`, visible on `/v1/metrics` while the job
  is running (same counter as aligned).
- Restart-fresh + `sealed` / finite: EOF sends the terminal watermark
  through `live_events`, final ET windows emit, then the ingress channel
  closes so the job can complete. No leftover open windows.

There is still no job-wide fail-on-decode switch (P1-17 remainder).

## Tests

- `n5_append_only_et_window_accepts_rows_after_eof_poll` — aligned
  AppendOnly ET tumble; chase to EOF; append later timestamps; the next
  window (`window_start=10s`) is produced (not all late).
- `n5_sealed_eof_emits_final_et_windows` — aligned Sealed; after EOF the
  last window emits and the job reaches `completed`.
- `n5_restart_fresh_sealed_emits_final_et_windows` — same finals +
  complete on the restart_fresh path.
- `n5_restart_fresh_decode_errors_counted` — bad NDJSON increments
  `decode_errors` while the job is running.
- Connector: `n5_contract_parse_and_eof_policy`,
  `n5_sealed_rejects_append_like_immutable`,
  `n5_poll_decoded_counts_bad_json`.

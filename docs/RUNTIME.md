# Runtime ownership (A3 / A4)

`compact_kernel()` builds a 2-worker Tokio runtime for tests and in-process
demos. It is **not** the production I/O pool.

`sparrow-server` uses `host_kernel()` (`available_parallelism`, clamped 4–16)
for mailbox / stage futures.

Blocking work must stay off those async workers:

| Work | Where it runs |
| --- | --- |
| SQLite catalog (`Store::*`) | `Store::run_blocking` → Tokio blocking pool |
| Checkpoint encode / fsync / CURRENT | `tokio::task::spawn_blocking` on the aligned commit path; metrics use the real duration and payload length (N15) |
| FileReplay `next_frame` / `decode_frame` | `spawn_blocking` batches of up to 32 frames or ~64KiB (N14), then back to async |

A host that already owns a Tokio runtime should construct `Kernel` with that
process's `Handle` in a later change; until then the Kernel still owns a
dedicated runtime, but SQLite/checkpoint/file no longer sit on the 2 compact
async workers.

`host_kernel()` still uses `ResourceBudget::compact()` (`max_state_keys = 1024`).
R2 batch 6 does not raise that production budget.

R2 batch 8: that budget is **process-wide**. `Kernel` holds one
`MemoryOwner`; jobs share it. Admit also tracks live mailbox reservations
so N jobs cannot each take a full compact queue. See
`docs/REVIEW_R2_BATCH8.md` (P1-20).

Aligned freeze encode writes from the live store (no full entry clone)
and refuses an oversized snapshot before CURRENT (P1-14). Residual: the
Kernel ACK still sends a `WindowFreeze` value (batch 9 left this; ACK →
encoded bytes is not a cheap supervisor/barrier change). Filter/project
and window aggs bind column names once (P2-30). Dedup expire pops a
deadline heap prefix (P2-32). Reference tables store a crc32 and fail
closed on mismatch (P2-36). See `docs/REVIEW_R2_BATCH9.md`.

Decode errors stay counted by default. `fail_on_decode` on the pipeline
spec, or `SPARROW_FAIL_ON_DECODE=1`, fails the job (P1-17).

MQTT `inbox_capacity × max_record` is rejected above 4 MiB **or** the
process `queue_bytes` (P1-27).

`sparrow-server` installs a stderr `tracing` subscriber (`RUST_LOG`, default
`info`). Aligned checkpoint commits log `checkpoint_commit` at info with id /
bytes / duration.

## OperatorId::WINDOW = 10 (layout fingerprints)

SQL/linear window nodes use `OperatorId::WINDOW` (**10**), assigned in R1 (A7).
A pre-R1 checkpoint that stored a different operator id (or a sequential
binder id) will fail closed on restore: `decide_state_reuse` rejects an
OperatorId mismatch. That is intentional — do not rewrite old CURRENT files.

## Secrets key (N12)

Catalog secrets are `enc:v2:` ChaCha20-Poly1305. The key is
`SPARROW_SECRETS_KEY` (32 raw bytes or 64 hex chars) or
`SPARROW_SECRETS_KEY_FILE`. If both are unset, the process uses a
**process-local random key** and logs a warning at store/server open
(dev only; sealed values will not survive restart). `--safe-mode` /
`SPARROW_SAFE_MODE=1` / `SPARROW_REQUIRE_SECRETS_KEY=1` refuse instead.

## Processing-time `now` (event-time future skew)

`WindowOperator::on_batch` / `WatermarkHub::observe_event` take `now` in
microseconds from `RuntimeClock`: host wall time on live jobs, or a
`SharedVirtualClock` in tests. That instant is **processing time**. It is
not event time and does not move the watermark.

When `max_future_skew` is set (ET windows default to one hour), a row
with `event_time > now + skew` is dropped, counted as
`RuntimeMetrics.future_dropped` (`GET /v1/metrics`), and must not advance
`max_et` (P0-5).


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

`host_kernel()` defaults to Performance-sized process memory caps and per-job
Compact quotas (`max_state_keys = 1024` per operator). `SPARROW_MAX_JOBS=N`
or server `--max-jobs N` (CLI overrides env; 1–256, default 16) sets process
reservation/retention/queue caps to N × the corresponding Compact job cap
(4/4/2 MiB). `host_kernel_with_max_jobs(N)` is the explicit embedding API.
No memory is preallocated by this setting. Child leases bill both ledgers;
admission checks both per-job and process queue capacity and space for every
admitted job's quotas. A plan exceeding its own queue quota is a configuration
failure (not retryable capacity contention).
`Kernel::new_with_job_budget` lets embedders set both budgets explicitly.

The root budget is **process-wide**. Each job's child `MemoryOwner` also
bills the root. Admission tracks live mailbox reservations and per-job
quotas so jobs cannot each take a full process budget. `budget()` returns
the process budget; `job_budget()` returns the job quota. `Kernel::new`
defaults to one quarter of process reservation/retention per job. The host
defaults (64 MiB process / 4 MiB per-job reservation and retention) admit
at most 16 jobs; queue capacity may impose a lower limit.

The production Kernel ACK carries leased encoded freeze bytes, not a
cloned `WindowFreeze`. Supervisor wraps those bytes in-place and calls
`commit_encoded` once; oversized state fails before CURRENT. Freeze bound,
reservation acquisition and encoding failures reject only the checkpoint,
not the running job. Structural checks enforce separate retention/reservation
caps; atomic lease acquisition checks current reservation headroom. Each
attempt has a fresh ID and a request-scoped ACK inbox; timeout (including
barrier injection), failure or cancellation drops queued snapshots, and late
ACKs cannot retain leases in an abandoned inbox. Filter/project
and window aggs bind column names at construction (P2-30). Dedup expire pops a
deadline heap prefix (P2-32). Reference tables store a crc32 and fail
closed on mismatch (P2-36).

Decode errors stay counted by default. `fail_on_decode` on the pipeline
spec, or `SPARROW_FAIL_ON_DECODE=1`, fails the job (P1-17).
File records above 64 KiB are discarded with bounded memory until newline
or finite EOF, then counted once as a decode error. AppendOnly EOF retains
discard progress; the durable cursor stays at the preceding record boundary
until the oversized record ends. A scan-budget yield is `FilePoll::Pending`,
never a terminal EOF, so huge records cannot monopolize a blocking worker
or prematurely finish a sealed stream. CRLF blank lines are ignored.

MQTT `inbox_capacity × max_record` is rejected above 4 MiB **or** the
process `queue_bytes` (P1-27).

R3 also moves synchronous management-API catalog operations to the blocking
pool, makes sink loss sticky across checkpoint retries, and keeps failure
counts until 30 seconds of stable running. Retry backoff is capped at 32
seconds. Retryable capacity admission failures do not consume the crash
cap; actual status is `waiting`, with a separate 500 ms → 1 s → 2 s → 4 s →
5 s capped capacity backoff. Successful start, explicit start, revision change
or stop clears the capacity retry state. Held jobs preserve their original
failure reason.

Capacity waiting requires the explicit `admission=capacity` error context in
addition to retryable `ResourceExhausted`; other resource errors remain failures.
Repeated capacity checks for the same waiting revision update status details
without appending `starting`/`waiting` history rows or increasing `attempt_id`.
A successful admission records a new running attempt. The history remains
globally bounded to 100 rows, but it no longer decides whether a job may restart.

Safe-mode reads the durable `actual_state.restart_blocked` latch. Recording a
failure sets the latch in the same transaction as the failed status; history
eviction, process restart, and crash-counter reset cannot clear it. An explicit
`POST /start` (also used by `/restore`) atomically commits desired running state
and clears both the latch and crash count. A successful run clears the latch too.

Catalog schema **v2** adds this latch. Opening a v1 catalog transactionally
migrates existing failure evidence from actual state and any retained history;
missing history falls back conservatively to the failure count/hold message.
V2 reopen never reconstructs a cleared latch from old logs. Back up the catalog
before upgrading; older binaries reject v2, so rollback requires the matching
pre-upgrade catalog backup. Checkpoint encoding and `FORMAT_VERSION=1` are unchanged.

`sparrow-server` installs a stderr `tracing` subscriber (`RUST_LOG`, default
`info`). Aligned checkpoint commits log `checkpoint_commit` at info with id /
bytes / duration.

## Compatibility and implementation limits

- Count windows expose `count_start` / `count_end`, replacing the former
  `window_start` / `window_end`. They are within-window arrival ordinals
  `[0, count)`, not timestamps. Update SQL/Graph/downstream mappings that
  explicitly name the old columns. PT/ET names are unchanged.
- `SPV1` / `MAN2` codec versions are unchanged; floating-point value codecs
  preserve exact bits. Restoring duplicate canonical state keys fails
  closed and requires reset/replay rather than overwriting one entry.
- The file library constructor defaults to `Immutable` for finite replay;
  Supervisor defaults to `AppendOnly` for tailing. Set `file_contract`
  explicitly when EOF behavior matters. AppendOnly retains partial lines;
  Sealed/Immutable deliver the final record even without a newline.
- Dynamic state keys remain unsupported by aligned snapshot codecs.
- Memory accounting uses size estimates, not a promised RSS limit. This
  is not a complete Scalar arena. MQTT inboxes still use bounded channels
  and static capacity checks rather than an independent lease.
- Default delivery remains `live_best_effort + restart_fresh`; no
  exactly-once or MQTT replay guarantee is added.

The root-level `Sparrow_Code_Review_R4.md` / `Sparrow_Code_Review_R5.md` record
reviews of earlier workspace states, not necessarily current behavior. Runtime
contracts above and regression tests describe the implemented fixes; historical
build/test counts are not release guarantees.

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

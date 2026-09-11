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

Legacy raw-row MQTT library ingress still rejects `inbox_capacity × max_record`
above 4 MiB or its queue budget (P1-27). The server now uses byte-accounted MQTT
ingress, described below; the static guard is not removed from the legacy API.

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

Repeated `/start` for the same **actually running** revision preserves its actual
status, revision and attempt ID; it does not launch a duplicate job. Other states
(including failed/waiting/completed) and revision changes still reset to stopped
for reconciliation. Explicit start continues to clear the crash count and latch.

Safe-mode reads the durable `actual_state.restart_blocked` latch. Recording a
failure sets the latch in the same transaction as the failed status; history
eviction, process restart, and crash-counter reset cannot clear it. An explicit
`POST /start` (also used by `/restore`) atomically commits desired running state
and clears both the latch and crash count. A successful run clears the latch too.

Pipeline status responses expose `actual.consecutive_failures`,
`actual.restart_blocked` and top-level `safe_mode`. The latch is persisted even
outside safe-mode, so `restart_blocked=true` alone does not mean auto-restart is
currently held. Likewise a running job may retain a nonzero crash count until
the 30-second stable-run threshold. `held:` messages distinguish safe-mode from
the consecutive-failure cap; do not treat a positive counter as a failed status.

Converge reads and validates actual state once per start candidate. A per-pipeline
state/hold-check error skips that pipeline (never authorizes its start), logs
`pipeline_converge_skipped` with its name and error, and continues other pipelines'
start/stop handling. Global failures such as reading the desired-state list log
`supervisor_converge_failed` and retry on the next iteration.

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

### Production transport and scheduling fixes (2026-09-11)

- **HTTP response consumption is a production fix**, not a benchmark-only
  adjustment. Returning on 2xx without consuming a nonempty HTTP/1.1 body can
  prevent connection reuse, causing repeated TCP connections and additional
  HTTPS handshakes. The sink now drains successful responses with a 64 KiB
  threshold and the existing request timeout. Oversized/truncated responses
  may discard the connection, but never retry a POST already accepted with
  2xx. Empty/204 responses are not evidence that the old nonempty-body path
  reused connections. Pooling still depends on the peer. Encoded payloads
  are now owned once and shared across retries, not copied on every attempt.
- **MQTT keepalive is independent of inbound traffic.** The PINGREQ deadline
  survives each `select!` iteration and advances after sending a ping.
  Continuous PUBLISH/PINGRESP reception and full inboxes cannot reset it.
  Previously rebuilding `sleep(keepalive / 2)` could suppress pings forever.
  The default keepalive is 30 s; the protocol's 1.5x timeout is 45 s (broker
  enforcement timing can vary). Ten-second trials did not exercise the fault.
  Idle and continuous-traffic regression tests require multiple pings on the
  same connection. See [MQTT 3.1.1 §3.1.2.10](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).
- **AppendOnly EOF no longer blocks checkpoint commands.** EOF returns a wait
  state; the source loop selects the 40 ms poll deadline alongside commands
  and cancellation. Checkpoint handling preserves the existing poll deadline.
  Blocking reads remain outside cancellable `select!` futures: a command must
  not abandon an in-progress read and lose its source cursor or rows.
- Live ingress shares an immutable schema and moves owned rows into batches.
  JSON batch encoding writes into the final buffer without per-row byte vectors.
  Type checks, credit limits, output order and failure semantics are unchanged.
- Ingest metrics count source batches once, including memory, ordered live and
  split-channel live input. Job completion/stop no longer adds live input a
  second time. This is processing-attempt ingress, not a unique-message delivery
  guarantee; snapshot/source-cursor counters are unchanged. `/v1/metrics` now
  exposes `io.mqtt_reconnects` and `io.http_retries`. `io_scope=running_attempts`
  makes clear that I/O totals cover currently supervised attempts, not durable
  history or a monotonic total across stopped/replaced jobs.

### MQTT burst handling

MQTT uses a bounded **wait-then-drop** policy when the inbox is full. Set
`source.inbox_wait_ms` in the pipeline JSON (MQTT only, integer 0..=1000):
omitted means **5 ms**; `0` restores the old immediate-drop behavior. Existing
stored specs remain readable and use the new default on their next start;
set `0` explicitly if immediate shedding is required. Embedders use
`MqttSourceConfig::inbox_wait_timeout`.
Older binaries reject an explicitly stored `inbox_wait_ms` field; account for
that when preparing a config/catalog rollback.

The fast path offers the current row without waiting. On Full, the single
source pump retains only that row and stops reading more packets. The raw-row
library path waits on a channel permit; the server also waits for byte credit
(a bounded 1 ms retry when shared credit is exhausted). A row stays outside
cancellable send futures and deadlines are not restarted by heartbeats. No extra
shadow queue or forwarding task is created. The encoded frame is freed before
waiting. See [Tokio Sender cancellation safety](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html#method.send).

`io.mqtt_backpressure_waits` counts entries into this wait, and
`io.mqtt_backpressure_recovered` counts rows subsequently enqueued. Actual
timeouts (or immediate Full at 0 ms) increment `io.mqtt_dropped_full`, not
temporary fullness. Stop/connection failure may abandon the current row;
these counters are diagnostics, not delivery receipts. Their scope remains
currently supervised attempts.

`mqtt_dropped_full` has always counted only this connector's local full-inbox
drops, **not total loss**. Under sustained overload, stopping socket reads can
increase latency in TCP/broker queues; each expired wait drops only the current
row before reading continues. Broker-side queue limits may instead drop data or
disconnect the subscriber. Monitor broker queues/drops/disconnects and exact
end-to-end delivery/age, not just this counter. Immediate-drop mode also does not
promise that the newest event is retained or enforce an event-age limit.

TCP receive buffers predate this policy but may be used more heavily now. They
are external kernel memory, outside Sparrow's queue ledger and process RSS;
broker memory is external too. For the isolated Linux benchmark host the observed
`tcp_rmem` was `4096 131072 6291456` and `tcp_moderate_rcvbuf=1`: initial receive
buffer 128 KiB, autotuning ceiling 6 MiB **per socket**, not a preallocation or a
whole-system memory cap. Sparrow does not currently set `SO_RCVBUF`. Recheck the
target host/network namespace, inspect per-socket `ss -m` and deployment memory
limits, and multiply external limits by actual connections. Do not assume that
6 MiB is universal. See [Linux TCP buffer controls](https://docs.kernel.org/networking/ip-sysctl.html#tcp-rmem-vector-of-3-integers-min-default-max).

This trades a bounded enqueue wait for better microburst tolerance; **5 ms is
not an end-to-end latency bound** (broker/socket/downstream queues and scheduler
delays still exist). Sustained overload may still drop locally or at the broker.
It does not add MQTT ACKs, durable buffering, replay, or a lossless guarantee.

### MQTT byte-accounted ingress and optional QUICKACK

The server always uses `run_budgeted` + `with_budgeted_live_io`. Pipeline
`source.inbox_bytes` defaults to **256 KiB** of decoded-row Queue credit;
`inbox_capacity` is still an independent item bound (1..=4096, unchanged default
16 in stored-spec decoding). Row credit counts Vec capacity, scalar storage,
nested strings/bytes/arrays/objects, Arc/header allowances and envelope overhead;
shared payloads are conservatively charged in full per row. This is a memory
estimate, not wire JSON length or an allocator/RSS guarantee.

The Kernel separately reserves a conservative channel slot/block allowance for
the whole receiver lifetime, including empty retained blocks, and includes it
in job/process admission. Configured payload bytes **plus** this allowance must
fit the job Queue budget and static 4 MiB ceiling. A 4096-slot inbox may therefore
need a smaller payload limit than 2 MiB. Dynamic acquisitions bill inbox, job and
process ledgers; sibling pipelines cannot bypass the shared limit.

The one decoded working row takes Reservation credit before waiting. Queue
admission acquires Queue credit before releasing that working lease. The Kernel
then acquires batch Reservation credit before releasing Queue credit. EOF,
cancellation, invalid rows and failed handoffs release leases through ownership.
Wire records remain <=64 KiB; decoded rows larger than the configured inbox or
Kernel single-batch/mailbox byte limit are explicitly dropped, not retried
forever. `fail_on_decode` concerns decoding errors, not an overload guarantee.
Embedding callers of `run_budgeted` must use the matching accounted receiver
path/owner (or reserve channel overhead for their own receiver lifetime).

New I/O diagnostics (scope: currently supervised attempts):
- `mqtt_inbox_items/bytes`: admitted rows, including rows being transferred to
  a batch until destination credit succeeds; not all Kernel mailboxes.
- `mqtt_inbox_metadata_bytes`: fixed channel allowance, separate from row gauges.
- `mqtt_inbox_peak_items/bytes`: per-attempt high watermarks; the aggregate is
  the **sum of per-attempt peaks**, not a simultaneous process peak.
- `mqtt_pending_bytes`: Reservation-billed working row awaiting admission.
- `mqtt_accounted_sources`: attempts using the accounted path.
- `mqtt_dropped_oversize`: decoded rows exceeding the single-row bound.
- `mqtt_dropped_budget`: byte-credit timeouts or working-credit failures.
  Byte-credit timeouts also count in `mqtt_dropped_full`; do not blindly sum
  overlapping reason counters. Snapshots of atomics are not transactional.

`source.tcp_quickack` is **false by default**, opt-in on Linux; explicit true
is rejected on other server platforms. A TCP transport wrapper rearms QUICKACK
after successful nonempty **socket** reads, below TLS, not per MQTT message or
TLS plaintext read. Verification of certificates/hostnames is unchanged. A
failed setsockopt increments `mqtt_quickack_errors` and disables further attempts
for that session; it does not drop messages. `mqtt_quickack_calls` exposes the
attempt count. Reconnect creates a fresh transport. The option is temporary
kernel state, not a permanent per-segment guarantee; evaluate CPU/ACK traffic
and latency on the actual network before enabling it.
See [Linux TCP_QUICKACK](https://man7.org/linux/man-pages/man7/tcp.7.html).

Deployment note: byte accounting is enabled by default on the server MQTT
path; QUICKACK is not. In the measured Linux loopback/Mosquitto setup, 10k/s
with QUICKACK off had about 40 ms p99, versus about 0.8 ms with it on. These
trials used 32 inbox slots, not the stored-spec default 16, and are not a
default-configuration lossless guarantee. For a colocated broker and latency-
sensitive workload, evaluate `source.tcp_quickack=true` or the broker's
`set_tcp_nodelay true`, changing only one side at a time. Measured server CPU
increased by roughly 2.2/6.6 percentage points of one core at 10k/20k; this is
the whole profile's cost, not isolated setsockopt overhead or a WAN forecast.

### HTTP coalescing, bounded concurrency and flush

HTTP sink tuning is opt-in and does not change delivery/replay guarantees:

| Pipeline `sink` field | Default | Contract |
|---|---:|---|
| `batch_rows` | 1 | 1..65536 coalescing **target**; an existing larger upstream batch is sent whole, not split |
| `batch_bytes` | 262144 | 2..1048576, hard JSON request-body bound including brackets; an oversized upstream batch fails delivery |
| `linger_ms` | 0 | 0..1000, flush-readiness deadline from the first batch; not a hard dispatch or end-to-end age bound |
| `max_inflight` | 1 | 1..8; **values above 1 explicitly allow delivery/completion reordering** |

Defaults retain one POST per upstream batch and one in-flight request. They add
a strict 256 KiB body bound: deployments with larger encoded upstream batches
must choose an appropriate `batch_bytes` and budget. `(batch_bytes + 512) ×
(max_inflight + 2)` must fit the static 4 MiB working-buffer ceiling. This is a
conservative configuration bound, not a permanently reserved amount. Actual
delivery credit is **buffer capacity + 512 bytes**, acquired incrementally:
512 bytes before encoding, then capacity growth before each allocation. Small
requests can fit small job quotas even with a large `batch_bytes` ceiling;
large requests still need enough job/process headroom alongside their input
batches and other working buffers. `compact_kernel` defaults to a
1 MiB per-job Reservation quota; production host jobs default to 4 MiB.

The collector merges only compatible schemas/owners, never clones source rows,
and flushes at the row target, byte boundary, fixed linger deadline or EOF.
An expired deadline makes the collector ready to flush; saturated request slots,
queued input/carry and scheduler delays can still postpone dispatch.
It holds at most one collector plus one carry batch and bounded request tasks.
The encoder checks bytes before final-buffer growth; the request buffer takes
incremental Reservation credit before growth and retains it across shared-body
retries. Capacity starts at up to 256 bytes and grows geometrically, clamped to
the hard body limit; unused capacity remains billed. Merge keeps both buffers
billed while acquiring destination growth. If that acquisition is denied,
the collector flushes the two groups separately through its carry path, with
no failed receipt merely because coalescing lacked headroom. Encoding admission
failure instead fails that input receipt and counts `http_budget_drops` (not a
JSON syntax error); partial buffers and credit are released. Allocation failure
may retain a conservative growth reservation until that delivery is dropped.
This bounds the final body, not every temporary serde allocation or RSS page.
Reservation is separate from the Retention ledger used by window/dedup state.

An aligned barrier requests a collector flush through `InflightCounter`, even
if the collector has not received the last enqueued batch yet. Every constituent
input batch receives exactly one success/failure settlement; grouping changes
POST count, not the number of required ACKs. The barrier still waits for all
prior successes and refuses failed cuts. EOF flushes/drains; explicit stop
cancels pending/in-flight work, fails unresolved receipts and joins workers.
Cancellation after an ambiguous transport write is not a delivery guarantee.

`http_posted` counts successful requests, `http_acked_batches` acknowledged
upstream batches, `http_dropped` failed/abandoned upstream batches.
`http_encode_errors` and `http_budget_drops` explain pre-send failures;
`http_failed/retries` retain transport-attempt semantics. Error reason counters
overlap drops. Existing 2xx drain/no-duplicate-retry handling remains unchanged.

At 20 ms RTT, the serial ceiling remains about 50 POST/s. Coalescing changes
rows/POST; concurrency can increase POST/s but may increase downstream pressure,
reordering and duplicate exposure on retry. Neither solves sustained overload,
durable replay or event freshness by itself; validate remote RTT independently.

### Other compatibility limits

- Top-level `queue_items` / `queue_bytes` are not currently updated by the
  production mailbox path (`record_queue` has no production callers). Treat
  these as unavailable (`queue_metrics_available=false`), not evidence of zero backlog. Ingest/emit/HTTP counters
  and connector loss/retry diagnostics remain separate observations; proper
  per-pipeline queue instrumentation is still required for queue-depth alerts.

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
- Memory accounting uses size estimates, not a promised RSS limit or complete
  Scalar arena. Server MQTT ingress is byte-accounted; legacy raw-row library
  ingress retains static bounds. Other mailbox gauges are not implicitly wired
  by the MQTT-specific metrics above. TCP/broker buffers remain external.
- Default delivery remains `live_best_effort + restart_fresh`; no
  exactly-once or MQTT replay guarantee is added.

### Production release gate

Passing unit/integration tests and short host benchmarks is not a universal
production certification. Before rollout, record the actual device/CPU/RAM,
concurrent rules, payload/key/state sizes, steady and burst rates, downstream
RTT and permitted loss. Validate those workloads with sustained runs, resource
headroom and recovery drills; a one-job loopback run does not certify 16 jobs,
remote sinks, large state or multi-day leak freedom. QoS0 best-effort is not
suitable for a requirement of lossless ingestion without a separate durable
delivery design. Configure a persistent secrets key, strong API credentials,
explicit data/target allowlists and appropriate transport security; alert on
drops/retries/reconnects, failed/held pipelines, disk headroom and process RSS.
Pin the tested binary/config, retain a catalog backup and define a compatible
rollback procedure. Keep invalid/overload trials in the evidence instead of
turning their partial delivery into a successful throughput number.

The root-level `Sparrow_Code_Review_R4.md` through `Sparrow_Code_Review_R6.md` record
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

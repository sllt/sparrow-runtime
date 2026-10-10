# Sparrow

Single-node **IoT/Edge streaming dataflow runtime**, V1.

Sparrow is dataflow-first: SQL and Graph share one typed IR. It is **not** a
distributed Flink clone and **not** a Rust eKuiper clone.

**Default delivery is `live_best_effort` + `restart_fresh` (`recovery=none`).**

- Event-time tumbling + hopping windows, watermarks, holdback, late side output
- Processing-time tumbling windows and count windows (arrival-order; they do **not** impersonate event-time)
- Window completion Preview: PT hopping, sliding count, per-event PT/ET sliding and bounded PT/ET sessions; **restart_fresh only**, see [`docs/WINDOWS.md`](docs/WINDOWS.md) for semantics and validation status
- Bounded analysis Preview: collection/encoding functions, UNNEST, two-source ET interval/window inner/left joins, six additional aggregates and an independently admitted finite-query API. See [`docs/ANALYSIS.md`](docs/ANALYSIS.md) for limits and matching validation; new operators/aggregates are **restart_fresh only**, not production-certified.
- Plugin Preview: native, QuickJS-ng JavaScript and fuel-bounded WASM scalar functions; standalone process SDK for Source/Sink/Transform. Immutable packages, Ed25519 publisher policy, exact dependencies, retained catalog references, explicit hash approval and management API/CLI. Backends are independently opt-in and restart-fresh only; native programs remain trusted service-UID code, not a multi-tenant sandbox. See [`docs/PLUGINS.md`](docs/PLUGINS.md) and [`docs/EXTENSIONS.md`](docs/EXTENSIONS.md).
- Incremental COUNT/SUM/AVG/MIN/MAX (checked integer overflow)
- Versioned as-of-event-time lookup in embedded plans (not eligible for current Server aligned restore)
- Aligned single-job checkpoint (`aligned`) for Replayable **File**: zero state, one Count/ET window, or two Count windows in a linear chain — **not** default exactly-once
- Recover only from verified committed checkpoints; missing/corrupt stores are rejected
- MQTT replay is **unsupported**; MQTT cannot pretend durable restore
- A default process restart is a **fresh attempt**, not restore
- `exactly_once` and generic `at_least_once` configs are **rejected**; the optional JetStream Preview has an explicit `checkpointed_at_least_once` contract

当前里程碑 / current milestone: **V1**（production aligned recovery + coordinator + observability）。

Runtime contracts and compatibility notes: [`docs/RUNTIME.md`](docs/RUNTIME.md).
K3 DAG Preview (Branch/Route/UnionAll, multiple I/O, bounded side outputs and a separate File graph checkpoint profile): [`docs/DAG.md`](docs/DAG.md).
K4 IoT Preview (change detection, deadband and hysteresis, bounded keyed state and profile-specific recovery): [`docs/IOT.md`](docs/IOT.md). Paused-time profiles v14/v15 cover one linear HoldFor or Debounce on File/JetStream with durable decision replay and required HTTP output IDs. The v16/v17 extension adds PT tumbling windows, Change/Deadband positive TTL and up to two linear state participants; see the [precise scope and matching validation status](docs/IOT.md#linear-time-completion). File time DAGs use separate v18/PT and v19/ET profiles with required HTTP outputs, deterministic rounds and bounded Union replay; see the [scope](docs/DAG.md#time-graph-recovery) and [matching validation](docs/PRODUCTION.md#time-graph-validation). These serialized, per-decision checkpoint profiles are not the high-throughput path. Advanced alarm lifecycle remains unfinished; no automatic snapshot migration or production certification is implied.

Managed reference tables Preview: immutable revisions, SHA-256 bindings, atomic CAS upsert/delete/rollback, optional per-batch hot following, and bounded asynchronous HTTP Lookup with ordered output and TTL caching. Live/remote lookups are restart-fresh only and require explicit restart after failure/process restart; they are not historical snapshots. Existing static profiles v8–v11 and dependency-aware GC remain unchanged. Catalog is v4; downgrade requires matching catalog/config backups. See [`docs/REFERENCE_TABLES.md`](docs/REFERENCE_TABLES.md) for contracts and [`docs/PRODUCTION.md`](docs/PRODUCTION.md) for matching evidence.
Non-blocking issues and optimization backlog: [`docs/OPTIMIZATION_BACKLOG.md`](docs/OPTIMIZATION_BACKLOG.md).
Action/File/function Preview: typed JSON output mapping, restricted HTTP query/MQTT topic templates, bounded Linux NDJSON File Sink, and 18 additional pure functions shared by SQL/Graph. These actions are **restart_fresh only**, not aligned output or cross-Sink transactions. See [`docs/ACTIONS.md`](docs/ACTIONS.md) and the [matching validation evidence](docs/PRODUCTION.md#actions-validation).
MQTT ingress now has decoded-byte accounting and optional Linux QUICKACK;
HTTP sinks support opt-in batch/linger and bounded concurrency (default serial).
Optional [durable HTTP output](docs/DURABLE_OUTPUT.md) adds bounded local persistence,
finite retries, output DLQ and authenticated replay/purge. Its receipt means local
commit, **not remote 2xx**; aligned scope is initially File/JetStream with zero
state or one legacy Count window. Default HTTP delivery remains unchanged.
See the runtime contracts before changing queue, body or ordering settings.
Server MQTT byte accounting is on by default (256 KiB payload credit); Linux
QUICKACK remains off. For latency-sensitive colocated Mosquitto deployments,
evaluate `source.tcp_quickack=true` against CPU/packet cost; loopback benchmark
results are not a guarantee for the default 16-slot inbox or a WAN.
HTTP defaults remain serial with one POST per upstream batch and a 256 KiB body
limit. Encoded buffers now reserve credit incrementally by capacity, not the
configured maximum body size; batching/concurrency still require explicit tuning.
Count-window boundary columns are now `count_start` / `count_end` (arrival
ordinals, not timestamps); PT/ET retain `window_start` / `window_end`.

## Quick start

Rust **1.98.0** is pinned in `rust-toolchain.toml` (edition 2021).
The production package profile is Linux x86_64; other targets need separate validation.

```bash
export SPARROW_TOKEN=dev-token

# Control plane + in-process MQTT broker and HTTP capture
cargo run -p sparrow-server -- --token "$SPARROW_TOKEN" --demo-io --catalog /tmp/sparrow.v01.db

# Default listen: http://127.0.0.1:43180
curl -s http://127.0.0.1:43180/v1/health
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" http://127.0.0.1:43180/v1/metrics
```

Then create a stream and a pipeline (see the curl examples in `scripts/m3-demo.sh`) and:

```bash
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" \
  -X POST http://127.0.0.1:43180/v1/pipelines/hot/start
```

`POST /start` commits **desired** state immediately. The supervisor starts
MQTT/HTTP afterwards. MQTT pipelines are still `restart_fresh`.

Host capacity defaults to **16 jobs**. Set `SPARROW_MAX_JOBS=32` or
`--max-jobs 32` (CLI wins; range 1–256) to scale process memory quotas.
Each job keeps a Compact quota: 4 MiB reservation, 4 MiB retention,
2 MiB queue and 1024 state keys per operator. Capacity-full pipelines show
`actual.status=waiting` and retry with 0.5–5 s backoff, without consuming
the crash cap. These are accounted-memory limits, not an RSS guarantee.

File/replay pipelines may set `"recovery":"aligned"` and restore from a
committed checkpoint via the **HTTP API** (`/start`, `/checkpoint`,
`/restore`, `/kill`). This is **not** exactly-once. MQTT + `aligned` is
still rejected.

### Production operations candidate

See [`docs/PRODUCTION.md`](docs/PRODUCTION.md) for fixed no-demo builds,
deployment templates, authentication, backup/upgrade/rollback and fault handling.
See [`docs/CAPACITY.md`](docs/CAPACITY.md) for reproducible capacity observations,
idle-latency tuning and the distinction between finite drain and sustained load.

```bash
bash scripts/production-build.sh /absolute/new/package
# Production client: no embedded Kernel or demo/testkit dependencies.
cargo build --locked --release -p sparrow-cli --bin sparrowctl --no-default-features
sparrowctl status hot
sparrowctl diagnose hot --output new-diagnostic.json
sparrowctl checkpoints hot
```

File + zero state, one eligible Count/ET window, or two Count windows can opt into periodic checkpoints with
`checkpoint.interval_ms`; `checkpoint.resume_latest=true` explicitly enables
automatic replay. Both are off by default. Waiter timeout does not cancel an
already-running durable filesystem commit. Numeric restore points are pinned
for the attempt/configuration that depends on them. None of this promises
exactly-once, a hard RPO, 72-hour stability or target-device certification.

K1 uses snapshot v3 with a complete participant manifest and a persisted state
generation. It does **not** automatically migrate R10's v1/v2 snapshots: keep the
old binary/backup or explicitly start fresh in a new checkpoint directory.
The Server refuses mixed legacy/v3 writes. RCP2 semantics permit changes after the last
stateful operator (stateless filters/projections included) while preserving the
source cut; source and all state dependencies must match. Older plain CP01 snapshots
retain full-computation matching. Source may continue after barrier injection,
but commits still require every participant and the real Sink flush.
Examples: `deploy/pipeline-k1-zero.json`, `deploy/pipeline-k1-two-count.json`.

`effective.aligned_eligible` is based on a bound plan. Without successful
binding it can be `null` (unknown), not optimistic `true`; clients must distinguish
unknown from eligible. Status describes the latest stored revision, not an older
running attempt. Check the reason field before enabling restore operations.

## Diagnose a running pipeline

```bash
curl -s -H "Authorization: Bearer $SPARROW_TOKEN" \
  http://127.0.0.1:43180/v1/pipelines/hot/status | \
  jq '{actual, observation, mailboxes}'
```

`observation` separates connection/progress, Source inbox, Sink outbox, HTTP
in-flight/encoded credit and bounded latency histograms for the **running
attempt**. A quiet input is not automatically unhealthy; File append EOF is
`waiting_for_append`. Unknown ages and insufficient percentile samples are null.
The old `queue_metrics_available=false` gauges remain unavailable: use the new
boundary views and Runtime `mailboxes`, not legacy zeroes. HTTP 2xx is not a
business acknowledgement, and histogram percentiles are bucket upper bounds,
not exact per-row p99. Contracts and test scope: [`docs/RUNTIME.md`](docs/RUNTIME.md).
Histogram bucket boundaries and quantile rules are shared once at the response's
top-level `histogram_contract` (status and metrics), rather than repeated inside
each histogram. Runtime progress counters are independently sampled; delivery
and queue conservation snapshots remain internally coherent.

## Build & test

```bash
cargo test --workspace

# V1 process demos (production aligned file checkpoint, MQTT reject, soak, API)
bash scripts/v1-demo.sh
bash scripts/review-fix-demo.sh
# Full-service benchmark (requires Mosquitto; builds the real server + driver)
bash scripts/bench.sh --mosquitto /path/to/mosquitto
cargo run -p sparrow-cli --bin v1_file_checkpoint -- --data FILE --chk DIR --mode gold
cargo run -p sparrow-cli --bin v1_mqtt_reject
cargo run -p sparrow-cli --bin v1_soak
# See docs/bench.md for eKuiper comparison, validation and measurement scope.

# V0.4 process demos (same File path; policy name is now aligned)
bash scripts/v04-demo.sh

# V0.1 API demo (starts a real sparrow-server, curl happy path + rejects + restart)
bash scripts/m3-demo.sh

# M2 closed loop (no control plane)
cargo run -p sparrow-cli --bin m2_mqtt_http_loop

# M1 / M0
cargo run -p sparrow-testkit --example m1_kernel_smoke
cargo run -p sparrow-testkit --example m1_sql_graph_equiv
cargo run -p sparrow-testkit --example m0_pipeline_smoke

# V0.2 / V0.3 process demos
bash scripts/v02-demo.sh
bash scripts/v03-demo.sh

bash scripts/test.sh
```

## V1 (honest)

Shipped: File/replay Source (message-boundary cuts, identity/rotation),
**production** aligned single-job checkpoint (versioned codecs,
OperatorId/StateSlotKey checks, coordinator timeout/abort/stop, recover
from committed only), Graph validate/explain, Connector SDK conformance,
capability matrix rejects, status `effective` guarantees, `/v1/metrics`.

**Not exactly-once.** MQTT restore is rejected. Session windows (L=0) and
local parallelism shards are optional/incomplete and do not block this
release.

**Not shipped:** WASM operator runtime (optional spike under
`experiments/wasm-spike/`, off default build), Graph Designer UI, session
late merge, retract, unrestricted/recoverable stream-stream join, distributed shuffle. Bounded fresh-only Join is described in [`docs/ANALYSIS.md`](docs/ANALYSIS.md). NATS JetStream is an optional, default-off Preview; see [`docs/JETSTREAM.md`](docs/JETSTREAM.md).

See `docs/v1-report.md`.

`sqlparser = "=0.62.0"` is used only by `sparrow-sql`.
`sparrow-runtime` does **not** depend on HTTP, SQLite, MQTT, Axum, Arrow, or SQL crates.

## Binary

| | |
|---|---|
| Name | `sparrow-server` |
| Bind | `127.0.0.1:43180` (loopback default; `--allow-remote` to override) |
| Auth | `Authorization: Bearer <token>` (`--token` / `SPARROW_TOKEN`) |
| Catalog | `--catalog PATH` (SQLite) |
| Safe mode | `--safe-mode` — do not auto-start pipelines whose last attempt failed; file paths require `SPARROW_DATA_ROOTS`; secrets key required |
| Demo I/O | `--demo-io` — embedded MQTT + HTTP capture (requires the `demo-io` Cargo feature, **default on**) |
| Data roots | `SPARROW_DATA_ROOTS` — colon-separated file/checkpoint allowlist |

Production / `--no-default-features`: `EmbeddedBroker`, `HttpCapture`, and `DemoHarness` are compiled out (`#[cfg(feature = "demo-io")]`). Runtime `--demo-io` then fails with `feature_unavailable`. Default features stay on so demos and `cargo test --workspace` keep the in-process broker.

Optional **JetStream Preview**: build with `SPARROW_JETSTREAM=1` using `scripts/production-build.sh`. It adds checkpoint-backed HTTP acceptance, stable output IDs, bounded replay and explicit failure for supported zero/Count-window pipelines; it is not a default dependency, distributed HA, a durable HTTP outbox or a production certification. See [the exact profile and recovery restrictions](docs/JETSTREAM.md).

R12 adds ready-input batching, bounded asynchronous Explicit ACKs, idle pull backoff and nonfatal checkpoint timeouts. The [reproducible broker/process tests and scoped NATS benchmarks](docs/JETSTREAM.md#r12-validation) distinguish backlog-drain throughput from sustained input capacity; 10k/s paced tests still show queueing.

Flags: `--bind` `--token` `--catalog` `--max-jobs` `--safe-mode` `--demo-io` `--allow-remote`.

**Safe-mode restart protection:** failure holds survive history pruning and process
restarts; an explicit `/start` clears the hold. Catalog schema v1 is automatically
migrated to **v2** on open. Back up the catalog before upgrading; older binaries
cannot open v2. See [`docs/RUNTIME.md`](docs/RUNTIME.md) for migration and retry semantics.

**File path allowlist (N3/R3):** `check_data_path` rejects every `..` component, then canonicalizes the existing prefix before comparing roots. The original path is never used as a `starts_with` fallback. This avoids symlink-plus-parent traversal ambiguities. CWD is never a default root (systemd cwd can be `/`). When `SPARROW_DATA_ROOTS` is unset, the only default is `{temp_dir}/sparrow` — not `/tmp` as a whole. `--safe-mode` / `SPARROW_SAFE_MODE=1` without `SPARROW_DATA_ROOTS` denies file paths. Demos should export `SPARROW_DATA_ROOTS` to their workdir.

**Secrets key (N12):** `SPARROW_SECRETS_KEY` (32 bytes or 64 hex chars) or `SPARROW_SECRETS_KEY_FILE`. Unset → process-local random key and a warning at catalog/server open (dev only). `--safe-mode` / `SPARROW_SAFE_MODE=1` / `SPARROW_REQUIRE_SECRETS_KEY=1` refuse. HTTP `header_secret` requires `https://` (same posture as MQTT credentials + TLS).

**Checkpoints vs pre-R1:** `OperatorId::WINDOW = 10` is part of the layout fingerprint. Pre-R1 snapshots with a different window operator id fail closed on restore.

## Workspace

```
crates/sparrow-model        IDs, types, errors, RowBatch, MemoryLease, WorkBudget
crates/sparrow-expr         expression IR, eval, numeric stride kernels
crates/sparrow-plan         GraphSpec, catalog, BoundLogical, physical fusion, explain, compat
crates/sparrow-sql          G0 gate + SQL → same BoundLogicalPlan
crates/sparrow-io           I/O contracts + ReplayableSource
crates/sparrow-formats      bounded JSON codec
crates/sparrow-connectors   MQTT source/sink, HTTP, File/replay source
crates/sparrow-runtime      Kernel, MemoryState, windows, aligned checkpoint, coordinator
crates/sparrow-control      SQLite catalog + desired→actual supervisor
crates/sparrow-server       authenticated /v1 API + sparrow-server binary
crates/sparrow-cli          composition-root demos
crates/sparrow-testkit      fixtures, virtual clock, M0/M1 demos
experiments/               G1a + optional WASM spike (not a workspace member)
docs/                      architecture, runtime contracts, ADRs, V1 report, benchmarks
```

## Invariants

- All buffers bounded (bytes + rows + work budget + mailbox items/bytes + keys + timers)
- V1 default delivery: `live_best_effort` + `restart_fresh` (`recovery=none` for PT/ET windows)
- `aligned` is opt-in, File/replay only, **not** exactly-once
- MQTT replay is **unsupported**; durable recovery configs are rejected
- Job-level failure attribution in-process; stop joins every chain task
- One engine; Compact / Performance are budgets
- `RowBatch` is the V0.1 default (ADR-003)
- Control plane and connectors stay **outside** `sparrow-runtime`

## Non-goals (V1)

Graph Designer UI product, WASM operator runtime, distributed execution,
exactly-once, session late merge, retract, stream-stream join, NATS,
multi-user RBAC, claimed SLOs.

See `docs/v1-report.md`.

Repository: https://github.com/sllt/sparrow-runtime

License: MIT

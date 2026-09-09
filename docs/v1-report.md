# Sparrow V1 report

Date: 2026-09-09  
Host: `cargo test --workspace` + `scripts/v1-demo.sh`

V1 is a **long-running Edge Runtime** with **explicit, capability-conditioned
recovery**. It is **not** distributed and **not** a stream database.

The R1 blueprint text itself is not in this repository. Semantics follow
blueprint §48 V1 plus the V0.4 experimental path that this release
stabilizes.

## Honesty (read first)

| Claim | Status |
|---|---|
| Default live contract | `live_best_effort` + `restart_fresh` (`recovery=none`) |
| `aligned` | Production ReplayableSource checkpoint. **Not exactly-once.** |
| Recover | Verified `CURRENT` + `MANIFEST` + chunk CRC only |
| Missing / corrupt checkpoint | **Rejected** (no silent empty-state continue) |
| Exactly-once / at-least-once | **Rejected** at parse |
| MQTT session / QoS>0 / dirty session | **Rejected** |
| MQTT + `aligned` | **Rejected** (capability matrix) |
| L=0 session window | **Optional / incomplete** (not a release blocker) |
| Local parallelism shards | **Optional / incomplete** (not sketched; not a blocker) |

## What shipped

| Piece | Where | Notes |
|---|---|---|
| Versioned snapshot/manifest codecs | `sparrow-runtime` `checkpoint.rs` | Magic `SPV1` / `MAN2`; `SNAPSHOT_VERSION=1` |
| OperatorId / StateSlotKey checks | snapshot layout + `restore_freeze` | Mismatch is a hard reject |
| Coordinator arbitration | `coordinator.rs` | Timeout / abort / stop; no stuck `Checkpointing` |
| Lookup table revision bind | `TableRevisionBind` on snapshot | Restore requires the same table name+revision |
| State-reuse white-list | `sparrow-plan` `compat.rs` | WHERE-before-window → reset/replay |
| Status effective guarantees | `GET /v1/pipelines/{name}/status` | `effective.recovery` + `recovery_risk` |
| Metrics | `GET /v1/metrics` | Job/connector counters; no per-event labels |
| Security | server + tests | Bearer token, loopback default, 64KiB body, target policy |
| Soak / fault | `v1_soak` + unit tests | Finite start/stop, kill/restore, disk-full, corrupt MANIFEST |

`sparrow-runtime` still has **no** Axum, SQLite, MQTT, HTTP client, or
`sqlparser` dependencies. `RowBatch` remains the default (ADR-003).

## Capability matrix

| Source | Replay | `restart_fresh` | `aligned` + checkpoint |
|---|---|---|---|
| `file` / `file_replay` | replayable | ok (no restore) | ok (production, not exactly-once) |
| MQTT | unsupported | ok | **reject** |
| HTTP push | unsupported | ok | **reject** |
| MQTT session claim | — | **reject** | **reject** |
| `exactly_once` | — | **reject** | **reject** |

## Compatibility / update

White-listed reuse (same OperatorId + StateSlotKey + window kind + keys +
aggregates + event-time field + holdback L + WHERE fingerprint + lookup
table revision):

- Restarting the same File/replay job against a committed checkpoint
- Changing a downstream log/HTTP sink (not encoded in the window layout)

Default **reset + replay** (do not reuse state):

- WHERE / filter **before** the window changes

Hard reject:

- OperatorId remapping without an explicit map
- StateSlotKey / window kind / keys / aggregates / table revision change
- MQTT or any `replay=unsupported` source claiming restore

### OperatorId mapping

Planner-assigned `OperatorId` values are the bind-time node ids
(`bind_graph` / `bind_linear`). V1 does **not** rewrite ids across
topology edits. If a Graph/SQL edit changes node order, the saved
checkpoint OperatorId will not match; restore is rejected and the
operator must reset/replay. There is no automatic remapping table in V1.

## Observability (budgeted)

`GET /v1/metrics` (bearer token) and structured log lines:

- `jobs_started` / `jobs_stopped`
- `ingested_rows` / `emitted_rows`
- `queue_items` / `queue_bytes`
- `watermark_lag_micros` (where a watermark exists)
- `checkpoint_duration_micros` / `checkpoint_bytes` / commits / aborts

No per-event or per-key labels.

## Security baseline

- Mutating and metrics calls require `Authorization: Bearer <token>`
- Default bind `127.0.0.1:43180`; non-loopback needs `--allow-remote`
- Request bodies capped at 64KiB
- `TargetPolicy` deny-by-default; unauthorized HTTP/MQTT hosts are 403
- TLS `skip_verify` rejected

## Optional / incomplete (do not block V1)

- **L=0 session window** — Graph/SQL `session` remains `FeatureUnavailable`.
  Gap-based session assigner + late merge are Future.
- **Local parallelism shards** — not present in the V0.4 sketch; not shipped.

## Explicitly OUT / Future (do not claim)

- Distributed shuffle / Raft / Kubernetes control plane
- General stream-stream join, retract / UDAF, default WASM, Graph Designer UI
- Exactly-once for arbitrary HTTP sinks without an idempotency contract
- Incremental / unaligned / multi-job checkpoints
- MQTT replay or session resume

## How to run

```bash
cargo test --workspace

bash scripts/v1-demo.sh

cargo run -p sparrow-cli --bin v1_file_checkpoint -- --data FILE --chk DIR --mode gold
cargo run -p sparrow-cli --bin v1_file_checkpoint -- --data FILE --chk DIR --mode checkpoint --until 2
cargo run -p sparrow-cli --bin v1_file_checkpoint -- --data FILE --chk DIR --mode restore
cargo run -p sparrow-cli --bin v1_file_checkpoint -- --data FILE --chk DIR --mode gold --window et
cargo run -p sparrow-cli --bin v1_mqtt_reject
cargo run -p sparrow-cli --bin v1_soak
```

| Demo | What it proves |
|---|---|
| `v1_file_checkpoint` | Production `aligned` File/replay; committed checkpoint; process kill; restore matches gold for count and ET windows |
| `v1_mqtt_reject` | MQTT stays `live_best_effort` + `replay=unsupported` |
| `v1_soak` | Finite start/stop, checkpoint/restore loops, disk-full and corrupt MANIFEST rejects |
| `scripts/v1-demo.sh` | Above plus `/v1/status` effective guarantees, `/v1/metrics`, unsupported configs 4xx |

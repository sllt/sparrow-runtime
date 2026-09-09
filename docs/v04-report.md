# Sparrow V0.4 report

Date: 2026-09-09  
Host: `cargo test --workspace` + `scripts/v04-demo.sh`

V0.4 is a **debuggable product with experimental recovery**. It is **not**
default exactly-once. MQTT without replay still cannot pretend durable
restore.

The R1 blueprint text itself is not in this repository. Semantics follow
the V0.4 checklist (recovery + Graph + connector SDK) plus V0.3 contracts.

## Experimental labels (read first)

| Claim | Status |
|---|---|
| Default live contract | `live_best_effort` + `restart_fresh` (`recovery=none`) |
| `experimental_aligned` | Opt-in single-job checkpoint. **Not production-ready.** |
| Exactly-once / at-least-once | **Rejected** at parse |
| MQTT session / QoS>0 / dirty session | **Rejected** |
| Recover from uncommitted chunks / ACK / `MANIFEST.tmp` | **Rejected** (committed CURRENT+MANIFEST only) |
| WASM operator offload | **Not shipped** (optional spike, off default build) |
| Web Graph Designer | **Not shipped** (offline validate/explain + CLI only) |

## What shipped

| Piece | Where | Notes |
|---|---|---|
| File / replay Source | `sparrow-connectors` `file_replay.rs` + `sparrow-io` `replay.rs` | NDJSON; message-boundary cuts; identity/rotation checks |
| Aligned single-Job checkpoint | `sparrow-runtime` `checkpoint.rs` + `aligned.rs` | Barrier between records; freeze+chunk write; ACK; manifest commit (rename); recover committed only |
| Crash-cut / fault inject | `FaultPoint` | During write (disk-full), after chunk, after ACK, after MANIFEST.tmp, after MANIFEST rename, checksum corrupt |
| Experimental label | `RecoveryPolicy::ExperimentalAligned` | `experimental` / `experimental_aligned` only — not `checkpoint` as a production policy name |
| Graph display / authoring | `sparrow-plan` `explain.rs`, `POST /v1/graphs/*`, `v04_graph_author` | Serve/validate/explain physical+fusion+time+state+guarantee; offline template. Not a Designer UI |
| Connector SDK conformance | `sparrow-connectors/tests/sdk_conformance.rs` | ReplayableSource + Sink flush vs MQTT unsupported |
| Capability matrix | `check_recovery_capabilities` | MQTT/http_push + experimental or restore → reject |
| WASM spike | `experiments/wasm-spike/` | Optional, **not** a workspace member, not a release blocker |

`sparrow-runtime` still has **no** Axum, SQLite, MQTT, HTTP client, or
`sqlparser` dependencies. `RowBatch` remains the default (ADR-003).

## Checkpoint protocol (experimental)

1. Caller aligns (no in-flight rows) and freezes operator `MemoryState`.
2. Payload is split into 4KiB chunks: write `NNNN.bin.part`, fsync, rename to `NNNN.bin`.
3. Write `ACK`.
4. Write `MANIFEST.tmp` (chunk CRC32s), fsync, rename to `MANIFEST`.
5. Write `CURRENT.tmp`, fsync, rename to `CURRENT`.
6. Restore reads CURRENT → MANIFEST → verified chunks only. Partial `.part`, missing ACK/MANIFEST, or checksum mismatch do **not** restore.

This is **not** Flink incremental checkpointing, **not** unaligned
barriers across multiple jobs, and **not** exactly-once sink handshakes.

## Capability matrix

| Source | Replay | `restart_fresh` | `experimental_aligned` + checkpoint |
|---|---|---|---|
| `file` / `file_replay` | replayable | ok (no restore) | ok (experimental) |
| MQTT | unsupported | ok | **reject** |
| HTTP push | unsupported | ok | **reject** |
| MQTT session claim | — | **reject** | **reject** |
| `exactly_once` | — | **reject** | **reject** |

## How to run

```bash
cargo test --workspace

bash scripts/v04-demo.sh

cargo run -p sparrow-cli --bin v04_file_checkpoint -- --data FILE --chk DIR --mode gold
cargo run -p sparrow-cli --bin v04_file_checkpoint -- --data FILE --chk DIR --mode checkpoint --until 2
cargo run -p sparrow-cli --bin v04_file_checkpoint -- --data FILE --chk DIR --mode restore
cargo run -p sparrow-cli --bin v04_mqtt_reject
cargo run -p sparrow-cli --bin v04_graph_author -- template
cargo run -p sparrow-cli --bin v04_graph_author -- explain graph.json
```

| Demo | What it proves |
|---|---|
| `v04_file_checkpoint` | Replayable file source; committed checkpoint; kill (process exit); restore positions+count-window state; finals match gold |
| `v04_mqtt_reject` | MQTT live_best_effort + replay=unsupported rejects session and experimental restore |
| `v04_graph_author` | V0.3 ET tumble GraphSpec validates and explains physical/fusion/time/state/guarantee |

Server (optional, same Graph surface):

```bash
# POST /v1/graphs/validate  and  POST /v1/graphs/explain
# Bearer token required. Body = GraphSpec JSON (catalog may be embedded).
```

## WASM spike (not a blocker)

See `experiments/wasm-spike/README.md`. Not in workspace members.
Host leaf `add_i64` only. No default-build dependency.

## What is NOT production-ready

- Experimental checkpoint durability across disks, fsync failures on all
  platforms, or multi-job alignment
- Exactly-once / two-phase sink commit / Kafka-style transactions
- MQTT replay or session resume
- Incremental / unaligned / changelog checkpoints
- Distributed restore, savepoint CLI productization
- Session late merge, retract, stream-stream join
- Full Graph Designer UI
- WASM operator runtime

## Remaining gaps vs V1

- Production (non-experimental) aligned checkpoint + savepoints
- Replayable *external* connectors beyond the file/test source
- Exactly-once sink protocol (if ever; not promised)
- Session windows, retract, stream-stream join
- Multi-input Graph merge (WM hub is still API-only)
- WASM production track
- Claimed SLOs

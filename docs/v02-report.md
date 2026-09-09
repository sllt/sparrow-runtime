# Sparrow V0.2 report

Date: 2026-09-09  
Host: `cargo test --workspace --locked` + `scripts/v02-demo.sh`

V0.2 is **done**. This is the first stateful milestone: processing-time
windows, incremental aggregates, task-owned `MemoryState`, bounded timers,
bounded dedup, static `ReferenceTable` enrichment, HTTP Push Source, and
MQTT Sink. It is still **not** event-time Flink and **not** crash recovery.

## What shipped

| Piece | Where | Notes |
|---|---|---|
| PT tumbling windows | `sparrow-runtime` `window.rs` | Assigned from injected process clock |
| Count windows | same | Emit when per-key count reaches N |
| Incremental COUNT/SUM/AVG/MIN/MAX | `aggregate.rs` | O(1) accumulator / key; never retain raw rows |
| Checked integer overflow | `ErrorCode::IntegerOverflow` | Fail the job; never wrap / saturate / NULL |
| Task-owned `MemoryState` | `state.rs` | Retention leases; `detach_copy` of keys/values |
| Bounded timers | `timer.rs` | Generation cancel; live-timer quota; heap compact |
| Bounded Deduplicate | `dedup.rs` | Requires TTL **and** `max_keys` |
| Static ReferenceTable | `lookup.rs` | Finite snapshot frozen on `JobRequest` |
| HTTP Push Source | `sparrow-connectors` `http_push.rs` | Outside runtime |
| MQTT Sink | `sparrow-connectors` `mqtt/sink.rs` | QoS 0; replay unsupported |
| SQL v0.2 | `sparrow-sql` `v02.rs` | `TUMBLE(PROCESSING_TIME, …)`, `COUNT_WINDOW(n)`, lookup JOIN |
| Graph nodes | `window_agg`, `count_window`, `dedup`, `lookup` | Event-time / hop / session / stream-stream join rejected |

`sparrow-runtime` still has **no** Axum, SQLite, MQTT, HTTP client, or
`sqlparser` dependencies. `RowBatch` remains the default (ADR-003).

## Integer overflow / NULL policy

- `COUNT(*)` counts every row, including all-NULL rows.
- `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX` skip NULL.
- If an aggregate sees only NULLs, the result is NULL (except `COUNT(*)`).
- Integer `SUM` / `COUNT` use **checked** add. Overflow is
  `integer_overflow` and fails the job.
- `Float64` uses IEEE addition (Inf/NaN may appear; not treated as overflow).

## Delivery / recovery honesty

V0.2 live contract is still `live_best_effort` + `restart_fresh`.

Processing-time window pipelines are advertised as **`recovery=none`**
(same policy as `restart_fresh`):

- A process restart opens **empty** windows.
- Open timers, keyed accumulators, and dedup maps are discarded.
- Results after a crash are **not** crash-identical.
- Checkpoint restore, MQTT session resume, and exactly-once remain rejected.

## How to run each demo

```bash
# Workspace tests (includes G3 evidence)
cargo test --workspace --locked

# All V0.2 process demos (asserts printed finals)
bash scripts/v02-demo.sh

# Individual processes
cargo run -p sparrow-cli --bin v02_pt_tumble_avg
cargo run -p sparrow-cli --bin v02_count_window
cargo run -p sparrow-cli --bin v02_bounded_dedup
cargo run -p sparrow-cli --bin v02_static_table
cargo run -p sparrow-cli --bin v02_http_mqtt_loop
```

| Demo | What it proves |
|---|---|
| `v02_pt_tumble_avg` | Virtual clock fires PT tumble; prints `FINAL a avg=15` / `b avg=30`; `recovery=none` |
| `v02_count_window` | Count window size 2 emits avgs 15 and 45 |
| `v02_bounded_dedup` | `ttl=0` / `max_keys=0` rejected; TTL-scoped run keeps 2 rows |
| `v02_static_table` | Job1 keeps snapshot v1=`west`; new Job2 sees v2=`east` |
| `v02_http_mqtt_loop` | Real HTTP POST → kernel filter → MQTT publish → subscriber |

## G3 results

Recorded by `sparrow-runtime` `g3_tests`:

1. **Incremental vs raw rows** — 200 temperatures for one key keep **1**
   state entry; retention bytes are far below the sum of raw row sizes.
2. **Key / timer quotas** — a third key or a third live timer is
   `resource_exhausted` (fail closed, no silent drop).
3. **Cleanup** — after `fire_due`, key count and retention return to 0;
   `cleanup()` cancels remaining timers.
4. **Virtual clock** — rows at t=0, advance 1s, two AVG finals (15 and 30).
5. **State detach** — UTF-8 keys are `detach_copy`; state does not alias
   the input batch `Arc<str>`.

## Remaining gaps vs V0.3 / later

Explicitly **not** done (do not pretend):

- Event-time, watermark, idle, lateness / holdback (V0.3)
- Checkpoint / aligned recovery (V0.4 / V1)
- Session late merge, retract, stream-stream join
- WASM, Graph Designer UI
- MQTT replay / exactly-once / at-least-once

OperatorId + `StateSlotId` keys are stable enough for a future restore
path, but V0.2 never loads them.

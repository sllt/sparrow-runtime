# Sparrow V0.3 report

Date: 2026-09-09  
Host: `cargo test --workspace` + `scripts/v03-demo.sh`

V0.3 is **done**. This is the event-time milestone: stream event-time
binding, per-input watermarks (idle/active, no going backward),
final-only lateness with output holdback (`wm_out ≤ wm_in - L`),
tumbling + hopping event-time windows (planner overlap cap), and
versioned as-of-event-time lookup. It is still **not** Flink, **not**
crash recovery, and **not** exactly-once.

The R1 blueprint text itself is not in this repository. Semantics follow
the V0.3 checklist (time + window chapters) plus the existing V0.2
contracts.

## What shipped

| Piece | Where | Notes |
|---|---|---|
| Event-time binding | `EventTimeBinding` + `WindowSpec.event_time_field` | Int64 / TimestampMicrosUTC micros |
| Per-input watermark | `sparrow-runtime` `watermark.rs` | Active/idle; uninitialized active blocks; all-idle does not advance |
| No WM backward | same | Per-input ignore; effective monotonic |
| Output holdback | `OutputHoldback` | `wm_out ≤ wm_in - L`; emit finals **then** advance |
| ET tumble | `window.rs` | Assign from event time, close on `wm_out >= end` |
| Hopping | `WindowKind::HoppingEventTime` | `ceil(size/slide)` planner-capped (`DEFAULT_MAX_HOP_OVERLAP=8`) |
| Late side output | `SharedCapture::late_rows` | After close; no retract / update |
| Versioned lookup | `VersionedReferenceTable` | `valid_from` slices; as-of event time |
| SQL v0.3 | `sparrow-sql` `v03.rs` / `bind_v03.rs` | `TUMBLE(ts, size, L)`, `HOP(ts, slide, size)`, `FOR SYSTEM_TIME AS OF` |
| Graph | `tumble_et` / `hop` / `event_time_field` / `temporal` | Session / retract / stream-stream join rejected |

`sparrow-runtime` still has **no** Axum, SQLite, MQTT, HTTP client, or
`sqlparser` dependencies. `RowBatch` remains the default (ADR-003).
NATS was **not** added (no existing pattern required it).

## Hard rule

Arrival-order / count / processing-time windows **must not** silently
impersonate event-time. Setting `event_time_field` or `lateness_micros`
on a count / PT tumble is `invalid_argument`. Event-time requires an
explicit `TumblingEventTime` / `HoppingEventTime` assigner plus holdback.

## Delivery / recovery honesty

V0.3 live contract is still `live_best_effort` + `restart_fresh`.

Event-time window pipelines are advertised as **`recovery=none`**:

- A process restart opens **empty** windows and drops watermarks.
- Open keyed accumulators and versioned-table *progress* are discarded
  (the versioned handle is job-scoped; it is not a checkpoint).
- Results after a crash are **not** crash-identical.
- Checkpoint restore, MQTT session resume, and exactly-once remain rejected.

## Anti-examples (`sparrow-runtime` `time_tests`)

1. Multi-input uninitialized WM — active input without a WM blocks effective.
2. Idle / active — idle inputs are excluded from the min.
3. All-idle — no progress (effective and progress are None).
4. Future timestamp — rejected when `max_future_skew_micros` is set.
5. WM monotonic — a lower punctuation is ignored.
6. Holdback — `wm_in=13s, L=3s` ⇒ `wm_out=10s`; advancing past the cap fails.
7. Late side output — after close, `t=8` is late; FINAL AVG=80 is emitted once.

## How to run each demo

```bash
cargo test --workspace

bash scripts/v03-demo.sh

cargo run -p sparrow-cli --bin v03_et_tumble_avg
cargo run -p sparrow-cli --bin v03_hop_overlap
cargo run -p sparrow-cli --bin v03_idle_active
cargo run -p sparrow-cli --bin v03_versioned_lookup
```

| Demo | What it proves |
|---|---|
| `v03_et_tumble_avg` | Injected times; window `[0,10)`, L=3s, key d1; FINAL AVG=80 once at e+L; LATE t=8 side output |
| `v03_hop_overlap` | Planner rejects overlap 9 > 8; legal hop size=10s/slide=5s emits `[0,10)` avg=80 |
| `v03_idle_active` | Two-input hub: uninitialized blocks, idle excluded, all-idle frozen, no backward WM |
| `v03_versioned_lookup` | as-of t=1s → west (v1); as-of t=6s → east (v2) |

## Multi-input limitation (honest)

Graph and SQL plans are still **one source → one sink** (fan-in/fan-out
rejected). Multi-input watermark idle/active is a first-class
`WatermarkHub` / window-operator API, demonstrated by `v03_idle_active`.
It is **not** a Graph merge node.

## Remaining gaps vs V0.4 / later

Explicitly **not** done (do not pretend):

- Checkpoint / aligned recovery (V0.4 / V1)
- Session late merge, retract, stream-stream join
- Exactly-once / at-least-once / MQTT replay
- WASM, Graph Designer UI, NATS
- Idle *timeout* inferred from processing time (V0.3 idle is explicit marks)
- Allowed-lateness *updates* after first final (V0.3 is final-only)

OperatorId + `StateSlotId` keys remain stable enough for a future restore
path, but V0.3 never loads them.

# Sparrow M2 report

Date: 2026-09-09  
Host: `cargo test --workspace` + `m2_mqtt_http_loop` on Rust 1.88+

M2 is **done**. This is a real I/O closed loop, not a mocked channel-only
unit test. Delivery remains **`live_best_effort` + `restart_fresh`**.

## What shipped (PR-08)

| Piece | Where | Notes |
|---|---|---|
| Bounded JSON codec | `sparrow-formats` | max size 64KiB / depth 8; schema validation; `BadRecordPolicy::{Drop,FailJob}` |
| MQTT JSON Source | `sparrow-connectors` | MQTT 3.1.1 QoS 0, `clean_session=true`, bounded inbox `try_send` |
| HTTP Sink + Log Sink | `sparrow-connectors` | reqwest **rustls** (verify on); bounded retries; LogSink ring buffer |
| Secrets / target policy | `SecretResolver`, `TargetPolicy` | deny by default; host:port allowlist; metadata / link-local blocked |
| TLS hooks | `TlsConfig` | `skip_verify` is always rejected; HTTPS uses rustls roots |
| Kernel live I/O | `JobRequest::{live_in,live_out}` | channels only — **no MQTT/HTTP crates in `sparrow-runtime`** |
| Composition root | `sparrow-cli` | wires connectors onto `Kernel::handle()` + shared cancel |
| Embedded broker + HTTP capture | in-process, cargo-testable | no external mosquitto required |

`sparrow-runtime` / `sparrow-model` still have no MQTT, HTTP, SQLite, Axum,
Arrow, or `sqlparser` dependencies. RowBatch remains the V0.1 default (ADR-003).

## How to run

```bash
# Full workspace (includes the live broker+HTTP+kernel test)
cargo test --workspace

# Mandatory live demo: starts broker + HTTP capture + Sparrow process
cargo run -p sparrow-cli --bin m2_mqtt_http_loop

# Still valid
cargo run -p sparrow-testkit --example m1_kernel_smoke
cargo run -p sparrow-testkit --example m1_sql_graph_equiv

bash scripts/test.sh
```

The demo does **not** need a system MQTT broker. An in-process MQTT 3.1.1
broker (`EmbeddedBroker`) binds `127.0.0.1:0`. A tiny HTTP/1.1 capture
server does the same. If you prefer an external broker later:

```bash
# optional — not required for CI
mosquitto -p 1883
# then point MqttSourceConfig.host/port at it and allowlist that target
```

## Live demo sequence

1. Reject unauthorized target, missing secret, QoS 1, dirty MQTT session,
   checkpoint / MQTT-session restore, and `TLS skip_verify`.
2. Start broker + HTTP capture + kernel (MemorySource←live_in, CaptureSink→live_out).
3. Publish the six fixture sensor JSON events on `sensors/json`.
4. Pipeline `WHERE temperature > 25` projects `device_id, temperature, ts`.
5. HTTP sink POSTs JSON; capture must contain:

```
edge-a | 26.2
edge-b | 31
edge-c | 29.4
```

6. Slow HTTP (100ms/request) + a 40–48 message QoS 0 flood on a **bounded**
   inbox (8) / outbox (4). Expect `mqtt_dropped_full > 0` **or** a capped
   decode count. VmRSS must not jump by tens of MiB in the demo window.
7. `JobHandle::stop` cancels kernel stages and connector tasks; `live_tasks == 0`.

## Capability matrix

| Connector | Replay | Live delivery | Restart | TLS |
|---|---|---|---|---|
| MQTT Source | **unsupported** | `live_best_effort` (QoS 0 only) | `restart_fresh` | skip_verify rejected; MQTT TLS transport not wired (plaintext loopback demo). HTTP path verifies. |
| HTTP Sink | unsupported | `live_best_effort` (bounded retries then drop) | `restart_fresh` | reqwest rustls, built-in roots, no `danger_accept_invalid_certs` |
| Log Sink | unsupported | `live_best_effort` | `restart_fresh` | n/a |

Configs that require durable recovery are **refused** at validate:

- MQTT QoS > 0 → `UnsupportedDelivery`
- `clean_session=false` → `UnsupportedRestore`
- `RestoreClaim::{Checkpoint,MqttSession,External}` → `UnsupportedRestore`
- `at_least_once` / `exactly_once` → `UnsupportedDelivery`
- unknown / non-allowlisted host:port → `PolicyDenied`
- missing named secret → `SecretMissing`
- `tls.skip_verify=true` → `PolicyDenied`

## Delivery honesty

V0.1 does **not** provide:

- exactly-once
- at-least-once
- MQTT persistent session / replay
- checkpoint / crash recovery
- restore of in-flight mailboxes

A full inbox **drops** the record and increments `mqtt_dropped_full`.
That counter is a diagnostic, not an ack. After `stop` or process exit,
the next attempt is empty (`restart_fresh`).

Do not describe this loop as “reliable ingest”.

## Buffer bounds

| Buffer | Cap (demo) | Overflow |
|---|---|---|
| MQTT source inbox | 8–16 items | `try_send` drop |
| Kernel mailbox | 8 items / 64KiB | backpressure + cancel-aware send |
| HTTP / live_out | 4–16 batches | sink send blocks; source then drops |
| HTTP retries | ≤ 2 (hard max 8) | then drop |
| JSON record | 64KiB / depth 8 | drop or fail job |
| MQTT packet | 128KiB | codec reject |
| Log ring | `max_lines` | evict oldest |

There is no unbounded `Vec`/`unbounded_channel` on the live path.

## Crate dependency rule

```
sparrow-cli ──► sparrow-connectors ──► reqwest / tokio net
     │                    │
     └──► sparrow-runtime ─┴──► (no MQTT, no HTTP, no SQLite)
```

`Kernel::handle()` lets the composition root spawn connector tasks on the
same Tokio runtime. The kernel only sees `mpsc::Receiver<Row>` /
`Sender<RowBatch>`.

## Evidence

| Check | Where |
|---|---|
| Filtered HTTP bodies | `sparrow-cli` test `mqtt_json_kernel_http_receives_filtered_rows` + demo bin |
| Broker decode | `sparrow-connectors` test `broker_source_decodes_json` |
| HTTP POST capture | `sparrow-connectors` test `http_sink_posts_to_capture` |
| Slow sink / RSS | demo step 4 + `slow_http_does_not_grow_unbounded` |
| Policy / secret / restore | demo step 1 + `rejects_are_explicit` |
| Graceful stop | demo step 5; `live_tasks == 0` |
| Kernel still I/O-free | `cargo tree -p sparrow-runtime --prefix none` must not list reqwest / rumqttc / axum / sqlparser / rusqlite |

## M3 gaps (do not pretend they are done)

1. **SQLite catalog** and durable pipeline revisions.
2. **Auth API** (this milestone has `SecretResolver` + allowlist only).
3. **Axum / control plane HTTP** — not started; do not put Axum in `sparrow-runtime`.
4. Windows, watermarks, checkpoint, exactly-once.
5. MQTT TLS *transport* (validation hook exists; demo uses loopback plaintext).
6. Fan-in / fan-out still `FeatureUnavailable`.
7. No operator-level metrics table beyond `IoDiagnostics` atomics.
8. No claimed SLOs.

M3 should add catalog + auth **outside** the runtime crate, the same way
M2 added connectors.

# Sparrow M3 report (V0.1)

Date: 2026-09-09  
Host: `cargo test --workspace` + `scripts/m3-demo.sh` on Rust 1.88+

M3 is **done**. This is a usable standalone V0.1: a real `sparrow-server`
process, a SQLite catalog, and an authenticated management API that starts
the same MQTT → kernel → HTTP loop as M2.

## What shipped (PR-09)

| Piece | Where | Notes |
|---|---|---|
| SQLite catalog | `sparrow-control` | streams, pipeline revisions, desired/actual, attempts, audit (capped), secrets, allowlist |
| Schema/format versions | `meta` table | `catalog_schema_version=1`, `format_version=1` |
| Supervisor | `sparrow-control` | converges desired→actual; **catalog commit does not wait for MQTT/HTTP** |
| Process restart | `reset_actual_after_process_restart` | actual jobs are gone; next start is a **new attempt** |
| `--safe-mode` | `sparrow-server` | do not auto-activate pipelines whose last attempt **failed** |
| HTTP API | `sparrow-server` `/v1/...` | Axum, bearer token, 64KiB body cap, default bind `127.0.0.1:43180` |
| If-Match | `PUT /v1/pipelines/{name}` | required on update (`rev-N`); 428 / 412 |
| Demo I/O | `--demo-io` | in-process MQTT broker + HTTP capture (CI-runnable) |

`sparrow-runtime` / `sparrow-model` still have **no** Axum, SQLite, MQTT, HTTP,
or `sqlparser` dependencies. RowBatch remains the V0.1 default (ADR-003).

```
sparrow-server ──► sparrow-control ──► rusqlite / connectors / sql
       │                   │
       └──► sparrow-runtime ┴──► (no Axum, no SQLite)
```

## Binary, ports, token

| Item | Value |
|---|---|
| Binary | `sparrow-server` (`cargo run -p sparrow-server`) |
| Default bind | `127.0.0.1:43180` (non-loopback refused unless `--allow-remote`) |
| Token | `--token` or `SPARROW_TOKEN` (**required**) |
| Catalog | `--catalog PATH` (SQLite file) or `:memory:` |
| Demo I/O | `--demo-io` (ephemeral MQTT + HTTP on `127.0.0.1:0`) |

Unauthenticated mutating calls return **401** `policy_denied`.
`GET /` and `GET /v1/health` are public and already state
`live_best_effort` / `restart_fresh`.

## How to run V0.1

```bash
# All workspace tests (includes API MQTT→HTTP loop)
cargo test --workspace

# Real process + curl happy path + rejects + server restart
export SPARROW_TOKEN=m3-dev-token
bash scripts/m3-demo.sh

# Manual
cargo run -p sparrow-server -- --token "$SPARROW_TOKEN" --demo-io --catalog /tmp/sparrow.v01.db

# Still valid
cargo run -p sparrow-cli --bin m2_mqtt_http_loop
cargo run -p sparrow-testkit --example m1_kernel_smoke
bash scripts/test.sh
```

### API happy path (with `--demo-io`)

```bash
export TOKEN=m3-dev-token
AUTH=(-H "Authorization: Bearer $TOKEN" -H "content-type: application/json")
BASE=http://127.0.0.1:43180

curl -s $BASE/v1/health
curl -s "${AUTH[@]}" -X PUT --data '{"fields":[...sensor fields...]}' $BASE/v1/streams/sensors
curl -s "${AUTH[@]}" -X PUT --data '{
  "stream":"sensors",
  "sql":"SELECT device_id, temperature, ts FROM sensors WHERE temperature > 25",
  "source":{"kind":"mqtt","use_demo_io":true,"topic":"sensors/json"},
  "sink":{"kind":"http","use_demo_io":true},
  "delivery":"live_best_effort",
  "recovery":"restart_fresh"
}' $BASE/v1/pipelines/hot

curl -s "${AUTH[@]}" -X POST $BASE/v1/pipelines/hot/start
# returns immediately: desired=running, actual may still be stopped
curl -s "${AUTH[@]}" $BASE/v1/pipelines/hot/status
curl -s "${AUTH[@]}" -X POST $BASE/v1/demo/publish-fixture
curl -s "${AUTH[@]}" $BASE/v1/demo/capture
curl -s "${AUTH[@]}" -X POST $BASE/v1/pipelines/hot/stop
```

`use_demo_io: true` resolves MQTT/HTTP to the **current** harness so a
process restart rematerializes connections. That is `restart_fresh`, not
session restore.

## Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| GET | `/` | no | index + honesty |
| GET | `/v1/health` | no | liveness + versions |
| GET | `/v1/capabilities` | yes | MQTT/HTTP replay=unsupported |
| POST | `/v1/validate` | yes | bind + I/O policy; no start |
| POST | `/v1/explain` | yes | physical stages / fusion |
| POST | `/v1/test` | yes | validate+explain, `started=false` |
| PUT/GET | `/v1/streams/{name}` | yes | schema CRUD |
| PUT/GET | `/v1/pipelines/{name}` | yes | revisioned spec; If-Match on update |
| GET | `/v1/pipelines/{name}/status` | yes | desired vs actual + attempt_id |
| POST | `/v1/pipelines/{name}/start\|stop` | yes | write desired; supervisor converges |
| PUT | `/v1/allowlist` | yes | TargetPolicy (deny by default) |
| PUT | `/v1/secrets/{name}` | yes | named secrets (M2 SecretResolver) |
| GET | `/v1/audit` | yes | last 200 actions |
| GET | `/v1/demo/io` | yes | `--demo-io` ports |
| POST | `/v1/demo/publish-fixture` | yes | 6 sensor JSON events |
| GET | `/v1/demo/capture` | yes | HTTP sink bodies |

## Delivery honesty (API)

Every validate / explain / status / error body includes:

- `delivery`: `live_best_effort`
- `recovery`: `restart_fresh`
- `replay`: `unsupported`
- `honesty`: process restart is a **fresh attempt**, not checkpoint restore

Rejected clearly (`422 unsupported_delivery` / `unsupported_restore`):

- `delivery=at_least_once` or `exactly_once`
- `restore.kind=checkpoint` or `mqtt_session`
- MQTT QoS > 0, `clean_session=false`, TLS `skip_verify`

Do **not** describe V0.1 as reliable ingest or crash recovery.

## Desired vs actual

1. `POST .../start` writes `desired_status=running` and returns.
2. Supervisor starts kernel + MQTT + HTTP on the runtime handle.
3. `actual_status` moves `stopped → starting → running` (or `failed`).
4. On process exit, in-memory jobs die. Boot calls
   `reset_actual_after_process_restart`. Definitions remain in SQLite.
5. If desired is still `running`, a **new** `attempt_id` is started
   (`restart_fresh`). `--safe-mode` skips pipelines whose last attempt
   `outcome=failed`.

## Capability matrix (V0.1)

| Area | Status |
|---|---|
| SQL SELECT/WHERE/CAST + GraphSpec | yes (shared IR) |
| MQTT JSON source QoS 0 | yes, live_best_effort, replay unsupported |
| HTTP / Log sink | yes, bounded retries / ring |
| Token auth + loopback bind | yes |
| SQLite catalog + revisions | yes |
| Windows / watermarks | **no** |
| Checkpoint / exactly-once | **no** (rejected) |
| Graph Designer UI / WASM | **no** |
| Distributed / fan-in-fan-out | **no** |
| Full RBAC | **no** (single bearer token) |

## Evidence

| Check | Where |
|---|---|
| API MQTT→HTTP filtered bodies | `sparrow-server` test `api_mqtt_http_loop_and_fresh_restart` |
| Unauthorized | `health_is_public_mutate_is_not` + `m3-demo.sh` |
| Restore / at-least-once | `restore_and_at_least_once_rejected` + demo |
| If-Match | `if_match_required_on_update` |
| Server process + restart | `scripts/m3-demo.sh` |
| Runtime still I/O-free | `cargo tree -p sparrow-runtime` has no axum/rusqlite/reqwest |

## Known non-goals / remaining gaps

1. No Graph Designer, no WASM, no UI beyond JSON.
2. No windows, joins, ORDER BY, or checkpoint.
3. MQTT TLS transport still not wired (validation hook + HTTP rustls only).
4. Single token, not multi-user auth.
5. Supervisor is in-process; a failed pipeline is job-level, not an OS jail.
6. `--demo-io` capture is process-local (lost on restart; that is honest).
7. No claimed SLOs.

V0.1 is a **single-node, live_best_effort** edge dataflow with a thin
control plane. Treat it as that.

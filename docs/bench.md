# Sparrow end-to-end bench

Host-specific product benchmark. **These numbers are not SLOs** and must not
be read as a latency or throughput guarantee. Re-run on the machine you care
about.

Does **not** claim exactly-once. MQTT replay remains **unsupported**.

## How to run

```bash
bash scripts/bench.sh
# or:
cargo run -p sparrow-cli --bin sparrow_bench --release
```

`scripts/bench.sh` builds the release `sparrow_bench` binary and prints one
block per scenario plus a loose smoke verdict.

## Scenarios

1. **`mqtt_filter_http`** — embedded MQTT broker → Filter (`temperature > 25`)
   / Project → HTTP capture sink. Reports sustained ingest rate, p50/p99
   end-to-end latency (publish timestamp vs HTTP capture), and connector
   drop / retry counters.
2. **`mqtt_http_queue_pressure`** — same path with a tiny inbox/outbox and a
   slow HTTP capture. Reports drop / backpressure counts. A blocked sink
   is expected to show queue pressure, not unbounded growth.
3. **`file_count_window`** — File/replay NDJSON → `COUNT_WINDOW` aggregate
   through an aligned session. Throughput for N events.
4. **`file_count_window_checkpoint`** — same file path with an aligned
   checkpoint every K events. Compare elapsed time / events/s against
   scenario 3. Aligned checkpoints are **not** exactly-once.

## Metrics in the report

| Field | Meaning |
|---|---|
| `events_per_s` | Completed events / wall time |
| `e2e_p50_us` / `e2e_p99_us` | Publish-to-HTTP-capture latency when timestamps are present |
| `drops` | MQTT full/bad + HTTP dropped |
| `backpressure` | HTTP inflight/retries or MQTT-full under a slow sink |
| `peak_rss_kb` | `/proc/self/status` VmRSS (Linux). Omitted on hosts without procfs |
| `smoke` | Loose regression gate, not a product SLO |

Default smoke thresholds (override only by editing `sparrow_bench.rs`):

- MQTT path: ≥ 5 events/s, p99 ≤ 5s, at least 25% of published events captured
- File path: ≥ 500 events/s for the configured N
- RSS ≤ 512 MiB

## Cloud VM run (2026-09-09)

Recorded on the Cloud Agent VM that produced this change. **Host-specific.**

```
(pending — filled after scripts/bench.sh runs on this VM)
```

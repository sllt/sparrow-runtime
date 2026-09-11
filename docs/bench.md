# Full-service benchmark v2

Host-specific measurements, **not SLOs, exactly-once, or MQTT replay guarantees**.
The old session/microbenchmark numbers are not comparable to these results.

## Run

Install Mosquitto and optionally unpack eKuiper. No Python driver is used.

```bash
# One quiet release build of driver + real server, then isolated service tests.
bash scripts/bench.sh --engine sparrow --out /tmp/sparrow-bench-new-run

# Same Mosquitto and Rust HTTP capture implementation for both engines.
SPARROW_BENCH_SKIP_BUILD=1 bash scripts/bench.sh --engine both \
  --ekuiper-dir /path/to/unpacked/ekuiper --out /tmp/compare-new-run

# Pilot only, not a performance claim:
SPARROW_BENCH_SKIP_BUILD=1 bash scripts/bench.sh --engine both \
  --ekuiper-dir /path/to/unpacked/ekuiper --quick --out /tmp/compare-pilot
```

`--out` must not already exist. Each run owns new engine/catalog/broker instances,
chooses loopback ports, copies eKuiper binaries/config into its run directory,
and terminates only its own children. Existing services and old reports are not
modified. Build logs go to `/tmp/sparrow-bench-build.log` by default.

Defaults: three measured trials plus a discarded warmup per scenario/engine;
10,000 MQTT inputs at 1000/s; 80,000 file inputs. Options include `--rounds`,
`--rate`, `--mqtt-events`, `--file-events`, `--checkpoint-events`, `--drain-secs`,
`--server-bin`, and `--mosquitto`. Event counts must be multiples of 8. Raise
counts until each measured trial is long enough for the target hardware;
`--quick` is only a wiring/correctness check. Alternate engine order by round.

Use `--scenarios` with comma-separated scenario names to run a subset without
rebuilding. `--window W` (default 8, multiples of 8 up to 10,000) controls file
windows; selected file counts must be divisible by W. `--sink-delay-ms` adds a
response delay to ordinary capture requests (pressure scenarios keep 8 ms).
This models sink response wait, not a real WAN's packet delay/loss. Optional
`--metrics-ms 100` retains periodic API snapshots for queue/ingest diagnostics;
sampling adds overhead and is disabled by default. Sparrow's current top-level
`queue_items`/`queue_bytes` gauges have no production update path: zero must not
be interpreted as an empty queue. Use actual ingest/emit/HTTP counters and the
controlled sink-delay/window experiments; snapshots alone do not prove an
outbox depth. `--drain-secs` is a deadline,
not an auto-scaled event-count budget: increase it for large/slow trials.

```bash
# After keepalive fixes: a 180 s measured continuous session, plus warmup.
SPARROW_BENCH_SKIP_BUILD=1 bash scripts/bench.sh --engine sparrow \
  --scenarios mqtt_filter_http --rounds 1 --mqtt-events 180000 --rate 1000 \
  --metrics-ms 100 --out /tmp/sparrow-continuous-new

# Lighter HTTP output: 800,000 inputs, only 1,000 window outputs, still exact-checked.
SPARROW_BENCH_SKIP_BUILD=1 bash scripts/bench.sh --engine both \
  --ekuiper-dir /path/to/ekuiper --scenarios file_count_window \
  --file-events 800000 --window 800 --out /tmp/sparrow-sink-light-new
```

The service is started with `--max-jobs 1`; reported RSS and capacity belong to
that configuration, not a default 16-job deployment. For rate sweeps use fresh
output directories and inspect every invalid trial; zero loss at one point is
not a capacity guarantee at a higher point or under bursty publication.

`--broker-nodelay true|false` makes the common broker's TCP_NODELAY setting
explicit (default false, preserving the original Mosquitto default). Compare
both settings when diagnosing burst delivery/tail latency, and retain the
setting with the results. This is a broker transport experiment, not an engine
optimization or permission to change existing services. Disabling Nagle may
reduce message latency at the cost of more TCP packets; see the
[Mosquitto configuration reference](https://mosquitto.org/man/mosquitto-conf-5.html).

## Workloads and timing

### Transport/batch/byte-budget matrix options

- `--quickack true|false`: Sparrow MQTT transport toggle (server default false).
- `--inbox-capacity N`, `--inbox-bytes N`: item cap and decoded-row credit;
  the new server additionally reserves channel metadata under job/process caps.
- `--http-batch-rows N`, `--http-batch-bytes N`, `--http-linger-ms N`,
  `--http-max-inflight N`: independent Sparrow HTTP overrides. Default is still
  one POST per upstream batch, one in-flight request. Concurrent delivery may
  reorder. Body-size errors/credit failures are failures, not success fallbacks.
- `--observe-ms 200`: opt-in Linux `ss -tinmp` snapshots scoped to this trial's
  MQTT/HTTP ports, plus engine/broker CPU ticks and broker RSS. The isolated
  broker enables `sys_interval 1`; a named `$SYS/broker/#` subscriber writes
  `broker-sys.jsonl`. This adds measurement/observer traffic and one connection;
  keep it identical in A/B. Raw socket records preserve endpoints/PIDs, send/
  receive queues, memory limits and TCP counters; distinguish the source from
  publisher, HTTP and `$SYS` sockets using endpoint/PID/client-ID evidence.
  These are **sampled** values, not exact queue peaks or packet/ACK traces.
  Missing topics/failed samples are unavailable, never zero. Broker counters
  include observer traffic, are broker-global, and may count different MQTT QoS
  paths; they cannot replace exact sink verification.
- `--checkpoint-before-sink true`: for `file_chunked_checkpoint`, wait for
  ingestion of each appended chunk, request checkpoint **before** collecting
  its HTTP output, then verify output. Use a long linger to test that the real
  barrier wakes a pending collector. Default false preserves the historical
  output-before-checkpoint baseline.

New overrides are recorded in `source_tuning` / `sink_tuning`, alongside
`observe_ms` and `checkpoint_before_sink`. Old servers reject explicit new
fields. Omitting overrides selects the chosen binary's defaults; retain hashes.
CPU values are Linux clock ticks (record `getconf CLK_TCK` before converting),
and external sampling files are named `*-sockets.jsonl`.

```bash
# Same broker, item cap and sink; change only --quickack for the paired trial.
SPARROW_BENCH_SKIP_BUILD=1 bash scripts/bench.sh --engine sparrow \
  --broker-nodelay false --quickack true --scenarios mqtt_filter_http \
  --rate 20000 --mqtt-events 200000 --rounds 3 --metrics-ms 100 \
  --observe-ms 200 --out /tmp/sparrow-quickack-new

# Explicitly unordered, batched slow-sink experiment, not a universal preset.
SPARROW_BENCH_SKIP_BUILD=1 bash scripts/bench.sh --engine sparrow \
  --quickack true --scenarios mqtt_filter_http --rate 20000 --mqtt-events 200000 \
  --sink-delay-ms 20 --http-batch-rows 512 --http-batch-bytes 65536 \
  --http-linger-ms 5 --http-max-inflight 4 --observe-ms 200 \
  --out /tmp/sparrow-batched-new
```

Application response delay is not network RTT emulation. QUICKACK loopback
results cannot certify WAN behavior, and larger queues cannot certify freshness.

`--inbox-wait-ms 0..1000` overrides **Sparrow MQTT** `source.inbox_wait_ms`.
Omitting it keeps the selected server's default (new server: 5 ms); `0` enables
the old immediate-drop strategy for same-binary A/B. The override is recorded
as `sparrow_inbox_wait_ms_override` in metadata/results (`null` means omitted,
not zero). It does not change eKuiper or file sources. Keep broker NODELAY,
queues, offered rate and sink latency fixed within each pair; inspect
`mqtt_backpressure_waits`, `mqtt_backpressure_recovered`, `mqtt_dropped_full`
in `--metrics-ms 100` snapshots alongside exact sink completeness and p99.
Wait-then-drop can reduce transient loss but is neither durable ingress nor an
end-to-end latency limit. Older servers reject this explicit new spec field;
omit the option when replaying a historical binary.

- **mqtt_filter_http**: persistent QoS0 publisher → shared Mosquitto → real
  service Filter/Project → HTTP capture. One quarter of inputs deliberately fail
  `temperature > 25`; every expected output sequence ID and projected value is
  checked. Latency uses the sender's monotonic time immediately before publish
  and the capture's full-request-body receive time, never its later polling time.
  Both timestamps are in the same driver process. MQTT setup is excluded.
- **file_count_window**: real file source / JSON decoder → Kernel/SQL pipeline →
  HTTP sink, not `AlignedSession` or its simplified parser. Keys change in
  contiguous blocks of W rows, so Sparrow keyed count-W and eKuiper stream count-W
  have exactly the same `(device_id, SUM(v))` results. For window index `w`, the
  expected key is `d(w % 8)` and sum is `W*W*w + W*(W-1)/2`
  (`64*w + 28` with the default W=8). Every output is verified.
  Timing starts before submitting the rule/pipeline configuration and ends at
  the last validated HTTP arrival; it includes activation, not daemon startup.
  A separate first-to-last-output rate describes the steady output interval.
  Inputs are generated immediately before each trial and are page-cache warm;
  this measures the processing/HTTP path, not cold-disk read bandwidth.
  One HTTP request per small window and one in-flight request can dominate this
  end-to-end rate. It is not an isolated compute-engine throughput number. Compare
  W=8 with sink-light W=800, and use diagnostic queue timelines before attributing
  a bottleneck; changing W also changes window semantics/work, not just transport.
- File sources remain open until the capture verifies all results. Sparrow uses
  AppendOnly; eKuiper's reread interval is one hour, longer than any trial.
  Source EOF or `lastStopTimestamp` is **not** evidence that the sink drained.
  eKuiper file rules use `disableBufferFullDiscard=true` to block rather than
  discard when buffers fill, matching Sparrow's file backpressure. MQTT retains
  best-effort/drop behavior. Queues are 32 per configured stage (stress: Sparrow
  8/2 vs eKuiper 8 per stage); stage counts and total buffered memory differ.
- **mqtt_http_queue_pressure**: unpaced input, smaller queues, 8 ms HTTP response
  delay. Actual missing/duplicate/invalid rows are reported. Loss is not silently
  accepted as a successful throughput comparison; these are labelled stress
  cases and excluded from the normal delivery success gate.
- **file_chunked_no_checkpoint / file_chunked_checkpoint** (Sparrow only): both
  use the real aligned pipeline, append the same 400-row chunks, and wait for each
  chunk's outputs. Only the latter invokes `/checkpoint` after each chunk. The
  matched baseline includes the same append/poll/gating overhead. These cuts
  have small/empty state, not a large-state checkpoint benchmark. Raw per-request
  checkpoint durations are retained; ratios must use nanoseconds, not rounded ms.
  EOF waits are now interruptible by checkpoint commands. The historical v2
  42.67 ms API duration included the former non-interruptible 40 ms source wait;
  it was not an isolated fsync duration or intrinsic checkpoint overhead ratio.
  Re-run paired cases after this fix rather than mechanically subtracting 40 ms.

## Transport, validation, memory

MQTT uses one connection per publishing session. HTTP capture supports keep-alive
and sends identical bodyless `204 No Content` ACKs in this benchmark. eKuiper
2.4.1 does not consume nonempty REST responses with `debugResp=false`, so a
nonempty ACK would otherwise defeat its pool. Sparrow's regression test also
checks pooling with a nonempty response;
the Sparrow sink drains bounded successful response bodies to enable pool reuse
without retrying already accepted POSTs. Request counts and TCP connection counts
are reported separately from **logical row counts**, including batched arrays.

`valid=true` requires all expected rows, zero duplicate/invalid rows, and zero
capture-buffer overflows. Missing results never fall back to source counters or
configured input counts: `input_events_per_s` becomes null on an invalid trial.
Normalized output hashes omit MQTT wall timestamps but include sequence IDs and
values; exact row validation is primary, hashes are only convenient comparisons.

The driver samples the **engine PID** RSS every 5 ms. It records baseline,
`sampled_peak_rss_kib`, process-lifetime `VmHWM`, sample count and raw CSV. Sampled
peaks may miss shorter spikes; VmHWM includes startup and earlier trials. Shared
broker, capture and publisher memory is excluded equally for both engines. This
is not a whole-deployment RSS comparison. Non-Linux memory fields are null.

Artifacts: `metadata.json`, `results.jsonl` (including warmups and invalid trials),
`summary.json` (valid-trial min/median/max), per-trial engine metrics and RSS CSV,
input fixtures, isolated configs, and process logs. Offered MQTT rate is a load
setting, **not** the engine's maximum sustainable throughput. Use a rate sweep
and accept only zero-loss trials before making capacity claims.

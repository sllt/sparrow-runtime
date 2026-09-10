# Review R2 — batch 2 (N4, N7)

P1 leftovers promoted to correctness. N1–N3 are already merged. N5–N16
are **not** in this batch.

## N4 — barrier ack must match `checkpoint_id`

The aligned wait loop used to accept any `WindowFrozen` / `SinkFlushed`.
A timed-out barrier left late acks in `ack_rx`. The next
`checkpoint_named` could pair that stale freeze+flush with the current
`pos_r` / `ingested_rows` — rows between the two cuts were in neither
frozen state nor the replay cursor.

Fix:

- `AlignedAck` always carries `checkpoint_id`; only `id == expected` is
  applied.
- On timeout / failed flush the expected id is abandoned (monotonic
  `next_id += 1`) and queued acks with a lower id are drained.
- Snapshot source position and ingested count are taken at barrier
  injection, not after a later stitch.
- Kernel `wait_outbox` timeout is a failed flush (`ok: false`), not a
  job death, so a later checkpoint can run after the sink recovers.

Test: `n4_stale_barrier_ack_not_used_for_next_checkpoint` (sparrow-control).

## N7 — HTTP 4xx / drop is not an aligned flush

`http.rs` used to `o.ack()` after every `post_batch`, including 4xx and
retries-exhausted drops (`http_dropped`). Aligned treated that as
`SinkFlushed` and would commit.

Fix:

- HTTP sink `ack()` only on a successful POST; `fail()` on drop/4xx.
- `AlignedAck::SinkFlushed { checkpoint_id, ok, dropped }`.
- Supervisor refuses commit unless freeze and flush ids match, `ok`,
  and `dropped == 0`.
- `/v1/metrics` `io.http_dropped` is exported.

Live `best_effort` still drops (outbox is `None` on that path).

Test: `n7_aligned_checkpoint_refuses_after_sink_4xx` (sparrow-control).

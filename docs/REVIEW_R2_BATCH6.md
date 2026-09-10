# Review R2 — batch 6 (N11–N16)

P2 leftovers plus cheap P3 misc. N1–N10 are already merged.
The large remaining ❌ backlog (A6 / P1-14 / P2-* / P3-*) is **not**
in this batch.

## N11 — closed-window flush is not a full scan per row

`take_closed_one` / `peek_closed_bytes` used to iterate every tumble
key and `to_vec()` the encoded key for `min_by_key`. Closing *k*
windows was O(n²) allocations.

Fix: `WindowOperator` keeps a `BTreeMap<(window_end, encoded_key), StateKey>`
updated on put/remove/restore. Take/peek pop the first entry with
`window_end <= wm`. Order is unchanged: smallest
`(window_end, encoded_key)` first.

Test: `n11_many_keys_close_in_deterministic_order` (256 keys, mixed
ends, mailbox chunks, order + completeness; 2s smoke bound).

## N12 — process-local secrets key is no longer silent

If `SPARROW_SECRETS_KEY` / `SPARROW_SECRETS_KEY_FILE` is unset, a
process-local random key is still used for **dev** (`enc:v2:`).
`Store::open` / `open_memory` and `sparrow-server` now log a warning.
`--safe-mode` / `SPARROW_SAFE_MODE=1` / `SPARROW_REQUIRE_SECRETS_KEY=1`
refuse at store open (same rule as seal/unseal). Secret-name AAD is
left for a later envelope version so existing `enc:v2:` blobs stay
readable.

Tests: `n12_unconfigured_secrets_key_warns_or_refuses`.

## N13 — `encode_json_batch` does not re-parse each row

The HTTP batch encoder concatenated by `encode_json_row` →
`serde_json::from_slice` → `to_vec(Array)`. It now writes `[` +
object bytes + `,` + `]`. Same JSON, one encode per row.

Test: `n13_encode_json_batch_is_array_without_reparse`.

## N14 — aligned file source batches `spawn_blocking`

`take_file_poll` did one `next_frame` + decode per blocking hop.
`poll_decoded_batch` / `take_file_batch` now read up to 32 frames or
~64KiB, then return to async. AppendOnly still polls on EOF (no
terminal watermark). Sealed/Immutable still emit one terminal
watermark and finish.

Tests: `n14_poll_decoded_batch_reads_frames_then_eof`,
`n14_append_only_batch_eof_then_growth`. Existing N5 EOF tests stay.

## N15 — checkpoint metrics are real duration and payload length

The aligned supervisor commit path recorded
`Duration::from_millis(1)` and `1` byte. `spawn_blocking` now returns
`(id, payload_len, elapsed)` from encode+commit into
`RuntimeMetrics` and `tracing::info!(checkpoint_commit)`.

Test: `n15_aligned_checkpoint_records_real_duration_and_bytes`
(bytes match `encode_with_max_state_keys` of the recovered snapshot).

## N16 — cheap misc

- `sparrow-server` installs a stderr `tracing` subscriber (`RUST_LOG`
  or `info`). Aligned commits log at info. The crate `tracing-subscriber`
  is not added (toolchain / lockfile stay on the existing edition2021
  graph).
- Unused `flush_closed` / `allow(dead_code)` removed.
- HTTP `header_secret` requires `https://` (and implies
  `tls.enabled`), same posture as MQTT username/password + TLS.
  Tests: `n16_http_header_secret_requires_https` (connector + validate).
- `OperatorId::WINDOW = 10` is the R1 layout id. Pre-R1 checkpoints
  with a different operator id fail closed on restore. Test:
  `n16_window_operator_id_is_ten`.
- `layout_from_physical` / `where_before_window_physical` take the
  last Filter **before** the window, not a trailing Filter after it.
  Tests: `n16_physical_layout_uses_filter_before_window`,
  `n16_layout_from_physical_uses_filter_before_window`.
- `host_kernel()` stays on `ResourceBudget::compact()` (1024 state
  keys). This batch does not raise production budgets.

## Tests

Named `n11_` … `n16_` plus `cargo test --workspace` and
`scripts/v1-demo.sh` / `scripts/review-fix-demo.sh`.

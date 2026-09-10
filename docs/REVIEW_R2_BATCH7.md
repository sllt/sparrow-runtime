# Review R2 — batch 7 (A6, A8, P0-8 remainder, P2-40, cheap P3)

N1–N16 are already merged. Do **not** reopen them.
Out of this batch: P1-14 freeze streaming, P1-20 process-wide
MemoryOwner, full A2 privatization, P2-30–39/41 perf rewrites.

## A6 — `demo-io` is a real feature gate

`EmbeddedBroker`, `HttpCapture`, and `DemoHarness` (and their exports)
are `#[cfg(feature = "demo-io")]`. The feature stays **default on** so
demos and `cargo test --workspace` keep the in-process broker.

Production:

```bash
cargo build -p sparrow-connectors --no-default-features
cargo build -p sparrow-control --no-default-features
cargo build -p sparrow-server --no-default-features
```

`sparrow-control` / `sparrow-server` / `sparrow-cli` depend on
connectors with `default-features = false` and re-enable `demo-io`
through their own default feature. A `--no-default-features` server
that is passed `--demo-io` at runtime returns `feature_unavailable`.

Tests: `a6_demo_io_exports_embedded_broker_and_http_capture`.

## A8 — GraphSpec deny-unknown + typed status

`#[serde(deny_unknown_fields)]` is on `GraphSpec`, `NodeSpec`, and the
nested catalog/window/agg/expr specs.

`PipelineStatus` is the Rust vocabulary for desired/actual. SQLite
columns stay `TEXT` (no catalog migration, no CHECK). Unknown strings
are rejected on write and on read. Desired writes accept only
`running` | `stopped`.

Tests: `a8_graph_and_node_spec_deny_unknown_fields`,
`a8_unknown_status_string_is_rejected`,
`a8_pipeline_status_rejects_unknown`.

## P0-8 remainder — Array / Struct / Map

Declared nested types validate against the schema instead of becoming
unchecked `Dynamic`. Depth is checked from bytes (string-aware) before
parse, and again during the visitor.

Tests: `p0_8_array_struct_map_validate_against_schema`,
`p0_8_depth_checked_from_bytes_before_parse`.

## P2-40 — JSON objects use `try_object`

JSON object decode no longer takes the last-wins `object()` path.
Duplicate keys fail closed (`InvalidArgument`).

Test: `p2_40_json_object_duplicate_keys_fail_closed`.

## Cheap P3

| ID | Fix | Test |
|---|---|---|
| P3-44 | `recovery_label` / explain.replay follow `RecoveryPolicy` + source kind; no hard-coded V0_3 | `p3_44_recovery_label_follows_policy_not_v0_3` |
| P3-45 | `SUM(Bool/Utf8)` rejected at bind | `p3_45_sum_rejects_bool_and_utf8` |
| P3-46 | Project nullability follows the expr (non-null input + preserving expr stays non-null) | `p3_46_project_nullable_*` |
| P3-47 | Count-window `window_start=0` / `window_end=count` are in-window arrival ordinals `[0, count)`, not event-time | `p3_47_count_window_bounds_are_ordinals_not_event_time` |
| P3-53 | `type_err` reports JSON *kind*, not the raw payload (must not land in SQLite `last_error`) | `p3_53_type_err_does_not_include_raw_payload` |
| P3-56 | `start_pipeline` does not `.ok()`-swallow body JSON; `ResourceExhausted` → 429, `Cancelled` → 409 | `p3_56_*` |
| P3-57 | `running` mutex is dropped before `stop_job().await` | (converge / kill / replace-job paths) |

## Tests

Named `a6_`, `a8_`, `p0_8_`, `p2_40_`, `p3_44_` … plus
`cargo test --workspace` and the existing demo scripts.

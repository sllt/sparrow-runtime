# Review R1 checklist (2026-09-10)

Mark `- [x]` only when the item is fixed **and** covered by a semantic test
(not an existence/closure-called stub). Each item ends with `→ fix note / commit`.

## Phase 1 — Stop the bleeding (P0)

- [x] **P0-1** aligned path honors WHERE + projection (or rejects dishonest plans) → Kernel keeps Filter; `validate_aligned_plan` does not strip. `p0_1_aligned_rejects_or_honors_where_projection` (v=1,2,3,4 + WHERE v>1 + COUNT_WINDOW(2) emits one window). `f19167e` / `be2a784`
- [x] **P0-2** PT / processing-time windows + aligned are rejected → `p0_2_pt_aligned_rejected`. `be2a784`
- [x] **P0-3** `POST /v1/pipelines/{name}/restore` starts the revision that contains `restore=checkpoint` → put then `request_start` latest; `p0_3_restore_after_prior_start_restores_checkpoint` (no second n=3 window after append+restore). `be2a784`
- [x] **P0-4** checkpoint barrier waits for real sink flush/ack (not channel-empty) → `InflightCounter` ack after HTTP I/O; kernel + supervisor `p0_4_barrier_waits_for_real_sink_flush`. `2348289` / `be2a784`
- [x] **P0-5** ET windows default `max_future_skew`; a year-2100 event must not poison later watermarks → `p0_5_future_skew_protects_watermark`. `f19167e` / `2348289`

## Phase 2 — Kill second executor (A1)

- [x] **A1** aligned runs the full `PhysicalPlan` through `Kernel::submit`; barrier freeze/ack in `stage_loop`; `AlignedSession` demoted from Supervisor → `a1_aligned_runs_full_plan_through_kernel`. `2348289` / `be2a784`

## Phase 3 — Blocking I/O off async workers (A3/A4)

- [x] **A3** production `Store::*` callers use `run_blocking` → supervisor catalog path; `r26_catalog_runs_off_async_worker` / `r26_slow_catalog_does_not_freeze_runtime`. `be2a784`
- [x] **A4** checkpoint fsync + FileReplay reads use the blocking pool; host/runtime documented → `spawn_blocking` on aligned commit/read; `host_kernel()`; `docs/RUNTIME.md`. `be2a784` / `f26732a`

## Phase 4 — Type / numeric correctness

- [x] **P0-6** `filter_mask` matches eval on Utf8 (error, not silent drop) → `p0_6_filter_mask_matches_eval_on_utf8`. `f19167e`
- [x] **P0-7** Int64/UInt64 exact compare beyond 2^53; Timestamp MIN/MAX; no NaN→Equal → `p0_7_int_compare_beyond_2_pow_53`. `f19167e`
- [x] **P0-8** JSON float→int64 errors; Bytes base64; Dynamic numbers fail closed → `p0_8_json_float_to_int_errors`. `f19167e`
- [x] **P0-9** (Float64, Int64) arith returns Float64; UInt64 infer/eval match; nullif/greatest/least consistent → `p0_9_float_int_arith_and_uint_infer_match_eval`. `f19167e`

## Phase 5 — Real ledger

- [x] **A2** `AlignedSession.finals/lates` bounded or unused on production path → production uses Kernel; session cap `MAX_SESSION_ROWS`; gold restore still matches. `2348289`
- [x] **P1-15** builders acquire reservation before Vec growth → `RowBatchBuilder::push`; `builder_rejects_unbounded_expansion` / `r04_small_reservation_rejects_unfinished_builder`. `f19167e`
- [ ] **P1-16** transform does not unaccounted `to_vec` of whole batches → code path fixed (`apply_steps`); no dedicated semantic test beyond existing transform jobs
- [x] **P1-20** `drain_watermark` is chunked take-emit-advance (R17 not bypassed) → `r17_take_closed_chunk_leaves_remaining_state` / `r17_many_keys_closing_chunk_within_mailbox`. `2348289`

## Phase 6 — Security

- [x] **P0-10** secrets AEAD (ChaCha20-Poly1305); no plaintext fallback in safe/production; length check fixed → `enc:v2:` via ring; `r27_secrets_are_sealed_and_not_plaintext`. `be2a784`
- [x] **P0-11** MQTT TLS wired; credentials require TLS; validate does not hardcode `tls: false` → rustls connect; `p0_11_mqtt_credentials_require_tls`. `2f3ad59` / `be2a784`
- [x] **P0-12** file/`checkpoint_dir` allowlist roots; HttpPush bind address policy → `p0_12_tmp_path_is_allowed` / `p0_12_http_push_rejects_unspecified_bind`. `2f3ad59`
- [x] **P1-29** unauthenticated requests do not wipe audit via sync SQLite → `p1_29_unauthenticated_does_not_write_audit`. `be2a784`

## P0 leftovers

- [x] **P0-13** snapshot decode bounds entries/bytes before `Vec::with_capacity(n)` → `p0_13_freeze_rejects_untrusted_capacity`. R2 N6: encode/decode share the job `max_state_keys` (codec default = performance 16384, not a hard-coded 4096). `n6_performance_budget_freeze_commit_recover_roundtrip`. See `docs/REVIEW_R2_BATCH4.md`.

## Remaining P1

- [ ] **P1-14** freeze clone bounded / documented → still full clone under `max_state_keys`; not incremental
- [ ] **P1-17** decode errors counted + optional fail policy → `IoDiagnostics.decode_errors` incremented; no job-wide fail-on-decode switch
- [x] **P1-18** `IoDiagnostics` on `RunningJob` and `/v1/metrics` → `r11_checkpoint_via_api_flushes_then_commits` metrics include `io`. `be2a784`
- [x] **P1-19** converge backoff; per-pipeline attempt cap; safe_mode clarified → `MAX_PIPELINE_ATTEMPTS=16` now applies to `consecutive_failures` (R2 N1). `r24_start_failure_is_actual_failed` / `n1_healthy_start_stop_cycles_not_held`. See `docs/REVIEW_R2_BATCH1.md`.
- [x] **P1-21** WorkBudget yields at quota (not reset every envelope) → `would_exhaust` + `begin_quantum`; `r02_quantum_allows_more_events_than_old_lifetime_cap`. `f19167e` / `2348289`
- [x] **P1-22** HTTP sink batch POST; do not retry 4xx → `http_sink_posts_to_capture` expects JSON array body. `2f3ad59`
- [ ] **P1-23** finite source emits final ET windows before cleanup → EOF sends a large ET watermark; no dedicated semantic test
- [x] **P1-24** recover previous generation on corrupt CURRENT; GC error after commit is not a failed commit → `p1_24_corrupt_current_recovers_previous_generation`; missing CURRENT still not a commit. `2348289`
- [x] **P1-25** `SNAPSHOT_VERSION` bumped when format changes (or documented unchanged) → format unchanged (decode caps only); version stays 1. `2348289`
- [x] **P1-26** PT restore: aligned+PT rejected (fail closed; no silent complete overdue) → covered by `p0_2_pt_aligned_rejected`. `be2a784`
- [ ] **P1-27** MQTT inbox byte bounds → `inbox_capacity * max_bytes ≤ 4MiB` in validate; no dedicated semantic test
- [x] **P1-28** single SQL binder dispatch is parse-driven (no `contains("HOP(")`) → `classify_statement`; `v03::tests::accept_hop`. `f19167e`

## Architecture A5–A8

- [ ] **A5** tracing in library crates (no `eprintln` JSON) → aligned checkpoint uses `tracing`; LogSink eprintln is product output
- [ ] **A6** demo broker / HttpCapture / DemoHarness feature-gated → `demo-io` feature exists (default on); not fully `cfg`-gated
- [x] **A7** stable OperatorId (kind constants / explicit Graph ids) → SQL/linear use `OperatorId::WINDOW` etc.; `window_id_is_stable_across_optional_filter`. Graph still uses author node ids. `f19167e`
- [ ] **A8** typed status + `deny_unknown_fields` on specs → `deny_unknown_fields` on specs; SQLite status remains a string

## P2 / P3 correctness-adjacent

- [x] **P3-42** PlanLayout agg fingerprint no duplicate ty/input → `p3_42_agg_fingerprint_includes_func_alias_and_input_once`. `f19167e`
- [x] **P3-43** PT/ET window kind tags are distinct → `p3_43_pt_and_et_window_kind_tags_differ`. `f19167e`
- [ ] **P3-45** SUM result types consistent → infer follows input numeric type; no extra rewrite
- [x] **P2-40** Dynamic object key collision fail-closed → `p2_40_duplicate_dynamic_keys_fail_closed`. `f19167e`
- [x] **P3-54** expr fingerprint is structured (not Debug) → `p3_54_expr_fingerprint_is_structured_not_debug`. `f19167e`

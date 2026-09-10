# Review R1 checklist (2026-09-10)

Mark `- [x]` only when the item is fixed **and** covered by a semantic test
(not an existence/closure-called stub). Each item ends with `→ fix note / commit`.

## Phase 1 — Stop the bleeding (P0)

- [ ] **P0-1** aligned path honors WHERE + projection (or rejects dishonest plans) →
- [ ] **P0-2** PT / processing-time windows + aligned are rejected →
- [ ] **P0-3** `POST /v1/pipelines/{name}/restore` starts the revision that contains `restore=checkpoint` →
- [ ] **P0-4** checkpoint barrier waits for real sink flush/ack (not channel-empty) →
- [ ] **P0-5** ET windows default `max_future_skew`; a year-2100 event must not poison later watermarks →

## Phase 2 — Kill second executor (A1)

- [ ] **A1** aligned runs the full `PhysicalPlan` through `Kernel::submit`; barrier freeze/ack in `stage_loop`; `AlignedSession` demoted from Supervisor →

## Phase 3 — Blocking I/O off async workers (A3/A4)

- [ ] **A3** production `Store::*` callers use `run_blocking` →
- [ ] **A4** checkpoint fsync + FileReplay reads use the blocking pool; host/runtime documented →

## Phase 4 — Type / numeric correctness

- [ ] **P0-6** `filter_mask` matches eval on Utf8 (error, not silent drop) →
- [ ] **P0-7** Int64/UInt64 exact compare beyond 2^53; Timestamp MIN/MAX; no NaN→Equal →
- [ ] **P0-8** JSON float→int64 errors; Bytes base64; Dynamic numbers fail closed →
- [ ] **P0-9** (Float64, Int64) arith returns Float64; UInt64 infer/eval match; nullif/greatest/least consistent →

## Phase 5 — Real ledger

- [ ] **A2** `AlignedSession.finals/lates` bounded or unused on production path →
- [ ] **P1-15** builders acquire reservation before Vec growth →
- [ ] **P1-16** transform does not unaccounted `to_vec` of whole batches →
- [ ] **P1-20** `drain_watermark` is chunked take-emit-advance (R17 not bypassed) →

## Phase 6 — Security

- [ ] **P0-10** secrets AEAD (ChaCha20-Poly1305); no plaintext fallback in safe/production; length check fixed →
- [ ] **P0-11** MQTT TLS wired; credentials require TLS; validate does not hardcode `tls: false` →
- [ ] **P0-12** file/`checkpoint_dir` allowlist roots; HttpPush bind address policy →
- [ ] **P1-29** unauthenticated requests do not wipe audit via sync SQLite →

## P0 leftovers

- [ ] **P0-13** snapshot decode bounds entries/bytes before `Vec::with_capacity(n)` →

## Remaining P1

- [ ] **P1-14** freeze clone bounded / documented →
- [ ] **P1-17** decode errors counted + optional fail policy →
- [ ] **P1-18** `IoDiagnostics` on `RunningJob` and `/v1/metrics` →
- [ ] **P1-19** converge backoff; per-pipeline attempt cap; safe_mode clarified →
- [ ] **P1-21** WorkBudget yields at quota (not reset every envelope) →
- [ ] **P1-22** HTTP sink batch POST; do not retry 4xx →
- [ ] **P1-23** finite source emits final ET windows before cleanup →
- [ ] **P1-24** recover previous generation on corrupt CURRENT; GC error after commit is not a failed commit →
- [ ] **P1-25** `SNAPSHOT_VERSION` bumped when format changes (or documented unchanged) →
- [ ] **P1-26** PT restore: aligned+PT rejected (fail closed; no silent complete overdue) →
- [ ] **P1-27** MQTT inbox byte bounds →
- [ ] **P1-28** single SQL binder dispatch is parse-driven (no `contains("HOP(")`) →

## Architecture A5–A8

- [ ] **A5** tracing in library crates (no `eprintln` JSON) →
- [ ] **A6** demo broker / HttpCapture / DemoHarness feature-gated →
- [ ] **A7** stable OperatorId (kind constants / explicit Graph ids) →
- [ ] **A8** typed status + `deny_unknown_fields` on specs →

## P2 / P3 correctness-adjacent

- [ ] **P3-42** PlanLayout agg fingerprint no duplicate ty/input →
- [ ] **P3-43** PT/ET window kind tags are distinct →
- [ ] **P3-45** SUM result types consistent →
- [ ] **P2-40** Dynamic object key collision fail-closed →
- [ ] **P3-54** expr fingerprint is structured (not Debug) →

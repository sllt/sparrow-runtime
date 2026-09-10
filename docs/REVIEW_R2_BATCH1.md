# Review R2 — batch 1 (N1–N3)

P0 leftovers from R1. N4–N16 are **not** in this batch.

## N1 — attempt cap is consecutive failures, not lifetime `attempt_id`

`attempt_id` still increments on start/stop/running/failed. The hold
(`MAX_PIPELINE_ATTEMPTS = 16`) applies only to `consecutive_failures`,
which resets to 0 when actual reaches `running` (or `completed`).
`request_start` / `request_start_at` clear the hold. When held, the
reason is written to `actual.last_error`.

Tests:

- `n1_healthy_start_stop_cycles_not_held` (sparrow-control)
- `n1_consecutive_failure_cap_holds_with_error` (sparrow-control)

## N2 — empty SQL is InvalidArgument, not a panic

`Parser::parse_sql("")` returns `Ok([])`. `bind_sql` / classify no longer
indexes `[0]` on an empty vec. `PipelineSpec::validate` / `basic_check`
reject blank or whitespace-only SQL.

Tests:

- `n2_empty_sql_is_invalid_argument` (sparrow-sql library + sparrow-control spec)
- `n2_put_empty_sql_is_4xx` (sparrow-server HTTP PUT stays 4xx; connection stays up)

## N3 — path allowlist: no lexical `..` bypass, no cwd default

`check_data_path` compares only the normalized/canonical path. The
`path.starts_with(root)` fallback is gone. Default roots never include
cwd. When `SPARROW_DATA_ROOTS` is unset, the only default is
`{temp_dir}/sparrow`. `--safe-mode` / `SPARROW_SAFE_MODE=1` without
`SPARROW_DATA_ROOTS` denies file paths. Demos set `SPARROW_DATA_ROOTS`.

Tests:

- `n3_path_traversal_rejected` (`/tmp/../etc/passwd`, nested `../`, symlink-out)
- `n3_cwd_not_default_root`

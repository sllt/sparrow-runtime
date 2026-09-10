# Runtime ownership (A3 / A4)

`compact_kernel()` builds a 2-worker Tokio runtime for tests and in-process
demos. It is **not** the production I/O pool.

`sparrow-server` uses `host_kernel()` (`available_parallelism`, clamped 4–16)
for mailbox / stage futures.

Blocking work must stay off those async workers:

| Work | Where it runs |
| --- | --- |
| SQLite catalog (`Store::*`) | `Store::run_blocking` → Tokio blocking pool |
| Checkpoint encode / fsync / CURRENT | `tokio::task::spawn_blocking` on the aligned commit path |
| FileReplay `next_frame` / `decode_frame` | `spawn_blocking` in the aligned source loop |

A host that already owns a Tokio runtime should construct `Kernel` with that
process's `Handle` in a later change; until then the Kernel still owns a
dedicated runtime, but SQLite/checkpoint/file no longer sit on the 2 compact
async workers.

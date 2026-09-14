#!/usr/bin/env bash
# No compilation, no existing service modifications. All mutations are inside
# a new directory and owned child process. kill -9 is process-crash, NOT power loss.
set -euo pipefail
for required in ss jq sha256sum timeout; do command -v "$required" >/dev/null || { printf 'missing required tool: %s\n' "$required" >&2; exit 2; }; done
package=$(cd "${1:?package}" && pwd)
root=${2:?new evidence directory}
test ! -e "$root"; mkdir -p "$root"; root=$(cd "$root" && pwd)
bin="$package/bin/sparrow-server"; ctl="$package/bin/sparrowctl"
test -x "$bin" && test -x "$ctl"
port=${SPARROW_SMOKE_PORT:-$((44000 + $$ % 10000))}
test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_URL="http://127.0.0.1:$port"
export SPARROW_TOKEN=production-isolated-smoke-not-a-deployment-secret
export SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef
export SPARROW_REQUIRE_SECRETS_KEY=1 SPARROW_DATA_ROOTS="$root"
pid=
cleanup() { if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; }
trap cleanup EXIT
expect_fail() {
    local want=$1 log=$2; shift 2
    if "$@" > "$root/$log" 2>&1; then printf 'unexpected success: %s\n' "$log" >&2; exit 1;
    else local code=$?; test "$code" -eq "$want"; fi
}
start_server() {
    "$bin" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" --safe-mode --max-jobs 2 >> "$root/server.log" 2>&1 &
    pid=$!
    for _ in $(seq 1 100); do
        if "$ctl" health > "$root/health.json" 2>/dev/null; then return; fi
        kill -0 "$pid"; sleep .1
    done
    printf 'server did not start\n' >&2; exit 1
}
status() { "$ctl" status check > "$root/status.json"; }
wait_state() {
    for _ in $(seq 1 100); do
        status
        if jq -e --arg state "$1" '.actual.status==$state and (if $state=="running" then .actual.revision==.desired.revision else true end)' "$root/status.json" >/dev/null; then return; fi
        sleep .1
    done
    printf 'state timeout: %s\n' "$1" >&2; return 1
}
wait_checkpoint() {
    for _ in $(seq 1 100); do
        status
        if jq -e '.checkpoint.succeeded_total>0' "$root/status.json" >/dev/null; then return; fi
        sleep .05
    done
    return 1
}
manual_checkpoint() {
    for _ in $(seq 1 100); do
        if "$ctl" checkpoint check > "$root/manual-checkpoint.json" 2>/dev/null; then return; fi
        sleep .03
    done
    return 1
}
expect_fail 1 demo-rejected.log timeout 10 "$bin" --demo-io --catalog :memory:
grep -q 'built without the demo-io feature' "$root/demo-rejected.log"
expect_fail 1 placeholder-token.log env SPARROW_TOKEN=REPLACE_WITH_A_RANDOM_MANAGEMENT_TOKEN timeout 10 "$bin" --catalog :memory:
grep -q 'production management token' "$root/placeholder-token.log"
start_server
expect_fail 1 duplicate-catalog.log timeout 10 "$bin" --bind "127.0.0.1:$port" --catalog "$root/catalog.db" --safe-mode
grep -q 'already owned' "$root/duplicate-catalog.log"
expect_fail 2 unauthenticated.json env SPARROW_TOKEN=wrong "$ctl" pipelines
expect_fail 1 plaintext-rejected.json "$ctl" --url http://192.0.2.1 health
"$ctl" capabilities > "$root/capabilities.json"
jq -e '.inventory.backend.jit==false and .inventory.aligned.periodic_checkpoint==true' "$root/capabilities.json" >/dev/null
printf '%s\n' '{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}' > "$root/stream-spec.json"
"$ctl" put-stream sensors "$root/stream-spec.json" > "$root/stream.json"
printf '%s\n' '{"device_id":"d1","v":10}' '{"device_id":"d1","v":20}' > "$root/events.jsonl"
jq -n --arg path "$root/events.jsonl" --arg chk "$root/checkpoint" '{stream:"sensors",
  sql:"SELECT device_id, SUM(v) AS s FROM sensors GROUP BY device_id, COUNT_WINDOW(3)",
  source:{kind:"file",path:$path,file_contract:"append_only"},sink:{kind:"log"},recovery:"aligned",checkpoint_dir:$chk,
  checkpoint:{interval_ms:200,timeout_ms:1000,retain_generations:3,max_store_bytes:33554432,resume_latest:true}}' > "$root/spec.json"
"$ctl" validate "$root/spec.json" > "$root/validate.json"
"$ctl" explain "$root/spec.json" > "$root/explain.json"
"$ctl" put-pipeline check "$root/spec.json" > "$root/put.json"
"$ctl" start check > "$root/start.json"
wait_state running; wait_checkpoint
"$ctl" diagnose check --output "$root/diagnostic.json" > "$root/diagnostic-stdout.json"
jq -e '.redaction | startswith("allowlisted_fields")' "$root/diagnostic.json" >/dev/null
test "$(stat -c %a "$root/diagnostic.json")" = 600
expect_fail 1 diagnose-clobber.json "$ctl" diagnose check --output "$root/diagnostic.json"
"$ctl" stop check > "$root/stop.json"; wait_state stopped
"$ctl" checkpoints check > "$root/checkpoints.json"
selected=$(jq -r '.storage.current' "$root/checkpoints.json")
test "$selected" != null
sha256sum "$root/checkpoint/CURRENT" > "$root/stopped-current.sha256"
sleep .4; sha256sum -c "$root/stopped-current.sha256" >/dev/null
"$ctl" restore check --snapshot-id "$selected" > "$root/restore-selected.json"
wait_state running
test "$(jq -r '.checkpoint.restored_from_checkpoint' "$root/status.json")" = "$selected"
# Fixed-point replay must not repeat automatically after a process restart.
# No third row has been appended yet: explicit re-arming is safe for this fixture.
kill -TERM "$pid"; wait "$pid"; pid=
start_server; wait_state stopped
sleep .6; status
jq -e '.actual.status=="stopped" and (.actual.last_error|contains("fixed snapshot"))' "$root/status.json" >/dev/null
cp "$root/status.json" "$root/fixed-restart-held.json"
"$ctl" start check > "$root/fixed-explicit-start.json"; wait_state running
test "$(jq -r '.checkpoint.restored_from_checkpoint' "$root/status.json")" = "$selected"
printf '%s\n' '{"device_id":"d1","v":30}' >> "$root/events.jsonl"
for _ in $(seq 1 100); do
    if sed -n 's/^sparrow-log //p' "$root/server.log" | jq -se 'length==1 and .[0].s==60' >/dev/null; then break; fi
    sleep .05
done
sed -n 's/^sparrow-log //p' "$root/server.log" | jq -se 'length==1 and .[0].s==60' >/dev/null
manual_checkpoint
"$ctl" stop check > "$root/stop-after-sum.json"; wait_state stopped
sha256sum "$root/checkpoint/CURRENT" > "$root/valid-current.sha256"
# Update is CAS-protected. Incompatible recovery never destroys CURRENT.
jq '.sql="SELECT device_id, SUM(v) AS s FROM sensors WHERE v > 5 GROUP BY device_id, COUNT_WINDOW(3)"' "$root/spec.json" > "$root/incompatible.json"
etag=$(jq -r .etag "$root/status.json")
"$ctl" put-pipeline check "$root/incompatible.json" --if-match "$etag" > "$root/put-incompatible.json"
"$ctl" restore check > "$root/restore-incompatible.json"; wait_state failed
cp "$root/status.json" "$root/rejected-status.json"
sha256sum -c "$root/valid-current.sha256" >/dev/null
"$ctl" stop check > "$root/stop-failed.json"; wait_state stopped
etag=$(jq -r .etag "$root/status.json")
"$ctl" put-pipeline check "$root/spec.json" --if-match "$etag" > "$root/put-compatible.json"
"$ctl" start check > "$root/restart.json"; wait_state running; wait_checkpoint
# Crash and restart with desired=running: explicit resume_latest is honored.
kill -9 "$pid"; wait "$pid" 2>/dev/null || true; pid=
start_server; wait_state running
jq -e '.checkpoint.restored_from_checkpoint != null' "$root/status.json" >/dev/null
printf '%s\n' '{"device_id":"d1","v":40}' '{"device_id":"d1","v":50}' '{"device_id":"d1","v":60}' >> "$root/events.jsonl"
for _ in $(seq 1 100); do
    if sed -n 's/^sparrow-log //p' "$root/server.log" | jq -se 'length==2 and .[1].s==150' >/dev/null; then break; fi
    sleep .05
done
sed -n 's/^sparrow-log //p' "$root/server.log" | jq -se 'length==2 and .[0].s==60 and .[1].s==150' >/dev/null
manual_checkpoint
"$ctl" stop check > "$root/cycle-stop.json"; wait_state stopped
before=$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 | wc -l)
for round in $(seq 1 20); do
    "$ctl" start check > "$root/cycle-start.json"; wait_state running
    "$ctl" stop check > "$root/cycle-stop.json"; wait_state stopped
done
after=$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 | wc -l)
test "$after" -le "$((before + 2))"
"$ctl" checkpoints check > "$root/final-checkpoints.json"
jq -e '(.storage.generations|length)<=3 and .storage.logical_file_bytes<=33554432' "$root/final-checkpoints.json" >/dev/null
# Graceful stop releases the catalog lock and preserves the selected point.
kill -TERM "$pid"; wait "$pid"; pid=
mkdir "$root/backup"
cp -a "$root/catalog.db" "$root/events.jsonl" "$root/checkpoint" "$root/backup/"
for suffix in -wal -shm; do
    if [[ -f "$root/catalog.db$suffix" ]]; then cp -a "$root/catalog.db$suffix" "$root/backup/"; fi
done
# Restore the actual backup, preserving original path identity. Keep the
# pre-restore originals separate; do not merely reopen the same live files.
mkdir "$root/before-backup-restore"
mv "$root/catalog.db" "$root/events.jsonl" "$root/checkpoint" "$root/before-backup-restore/"
for suffix in -wal -shm; do
    if [[ -f "$root/catalog.db$suffix" ]]; then mv "$root/catalog.db$suffix" "$root/before-backup-restore/"; fi
done
cp -a "$root/backup/catalog.db" "$root/backup/events.jsonl" "$root/backup/checkpoint" "$root/"
for suffix in -wal -shm; do
    if [[ -f "$root/backup/catalog.db$suffix" ]]; then cp -a "$root/backup/catalog.db$suffix" "$root/"; fi
done
start_server
"$ctl" restore check > "$root/backup-read-restore.json"; wait_state running
printf '%s\n' '{"device_id":"d1","v":70}' '{"device_id":"d1","v":80}' '{"device_id":"d1","v":90}' >> "$root/events.jsonl"
for _ in $(seq 1 100); do
    if sed -n 's/^sparrow-log //p' "$root/server.log" | jq -se 'length==3 and .[2].s==240' >/dev/null; then break; fi
    sleep .05
done
sed -n 's/^sparrow-log //p' "$root/server.log" | jq -se 'map(.s)==[60,150,240]' >/dev/null
"$ctl" stop check > "$root/final-stop.json"; wait_state stopped
kill -TERM "$pid"; wait "$pid"; pid=
jq -n --argjson fd_before "$before" --argjson fd_after "$after" '{status:"PASS",scope:"isolated_local_process_not_soak_or_power_loss",
    automatic_checkpoint:true,fixed_restore_restart_held:true,explicit_start_preserves_fixed_point:true,
    selected_restore_sum:60,process_restart_sum:150,backup_restore_sum:240,lifecycle_cycles:20,
    no_extra_output:true,fd_before:$fd_before,fd_after:$fd_after,
    negative:["demo","placeholder_token","duplicate_catalog","auth","remote_plaintext","diagnostic_clobber","incompatible_restore"],
    sigterm_join:true,netem:"NOT_RUN",long_soak:"NOT_RUN"}' > "$root/summary.json"
printf 'PRODUCTION_SMOKE_OK %s\n' "$root"

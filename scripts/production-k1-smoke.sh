#!/usr/bin/env bash
# Isolated real-process K1 zero/two-state crash/recovery. No build or deployment.
set -euo pipefail
for tool in jq ss date sha256sum curl; do command -v "$tool" >/dev/null || { printf 'Missing smoke dependency: %s\n' "$tool" >&2; exit 2; }; done
repo=$(cd "$(dirname "$0")/.." && pwd)
package=$(cd "${1:?package}" && pwd); root=${2:?new evidence directory}; baseline=${3:-}; pre_r11=${4:-}
test ! -e "$root"; mkdir -p "$root"; root=$(cd "$root" && pwd)
server="$package/bin/sparrow-server"; ctl="$package/bin/sparrowctl"
test -x "$server" && test -x "$ctl"
# Stay below Linux's usual ephemeral range used by concurrent test listeners.
port=${SPARROW_SMOKE_PORT:-$((20000 + $$%9000))}; test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_URL="http://127.0.0.1:$port" SPARROW_TOKEN=k1-isolated-smoke-not-a-deployment-secret
export SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef SPARROW_REQUIRE_SECRETS_KEY=1 SPARROW_DATA_ROOTS="$root"
pid=
cleanup() { if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; }
trap cleanup EXIT
start_server() {
    local catalog_path=${catalog_path:-"$run/catalog.db"}
    "$server" --bind "127.0.0.1:$port" --catalog "$catalog_path" --safe-mode --max-jobs 1 >> "$run/server.log" 2>&1 & pid=$!
    for _ in $(seq 1 100); do
        if "$ctl" health > "$run/health.json" 2>/dev/null; then return; fi
        kill -0 "$pid"; sleep .05
    done; return 1
}
status() { "$ctl" status check > "$run/status.json"; }
wait_status() {
    for _ in $(seq 1 100); do status; if jq -e "$1" "$run/status.json" >/dev/null; then return; fi; sleep .05; done
    printf 'K1 status deadline: %s\n' "$1" >&2; return 1
}
checkpoint() {
    for _ in $(seq 1 100); do if "$ctl" checkpoint check > "$run/checkpoint.json" 2> "$run/checkpoint-error.json"; then return; fi; sleep .03; done
    return 1
}
for shape in zero two-count; do
    run="$root/$shape"; mkdir "$run"
    catalog_path="$run/catalog.db"
    start_server
    printf '%s\n' '{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}' > "$run/stream.json"
    "$ctl" put-stream sensors "$run/stream.json" > "$run/put-stream.json"
    initial=6; added=1; field=v; expected='[200,101]'; input='1 2 3 4 5 200'; append='101'
    if [[ "$shape" == two-count ]]; then initial=7; added=5; field=total; expected='[21,57]'; input='1 2 3 4 5 6 7'; append='8 9 10 11 12'; fi
    for v in $input; do printf '{"device_id":"d1","v":%s}\n' "$v"; done > "$run/events.jsonl"
    jq --arg path "$run/events.jsonl" --arg checkpoint "$run/checkpoint" \
        '.source.path=$path | .checkpoint_dir=$checkpoint | .checkpoint.interval_ms=null | .checkpoint.timeout_ms=1000' \
        "$repo/deploy/pipeline-k1-$shape.json" > "$run/spec.json"
    "$ctl" validate "$run/spec.json" > "$run/validate.json"
    "$ctl" explain "$run/spec.json" > "$run/explain.json"
    states=0; if [[ "$shape" == two-count ]]; then states=2; fi
    jq -e --argjson n "$states" '.effective.aligned_eligible and (.effective.checkpoint_participants.states|length)==$n' "$run/explain.json" >/dev/null
    "$ctl" put-pipeline check "$run/spec.json" > "$run/put.json"
    if [[ "$shape" == zero ]]; then
        mkdir -p "$run/checkpoint/STATE_GENERATION.tmp"
        "$ctl" start check > "$run/generation-failure-start.json"
        wait_status '.actual.status=="failed"'
        curl --silent --show-error --fail -H "Authorization: Bearer $SPARROW_TOKEN" "$SPARROW_URL/v1/metrics" > "$run/generation-failure-metrics.json"
        jq -e '.jobs_started==0 and .ingested_rows==0' "$run/generation-failure-metrics.json" >/dev/null
        test ! -e "$run/checkpoint/CURRENT"
        rmdir "$run/checkpoint/STATE_GENERATION.tmp"
    fi
    "$ctl" start check > "$run/start.json"
    wait_status ".actual.status==\"running\" and .observation.runtime_progress.ingested_rows==$initial"
    checkpoint; status
    generation=$(jq -r .checkpoint.state_generation "$run/status.json")
    [[ "$generation" =~ ^[0-9a-f]{32}$ ]]
    sed -n 's/^sparrow-log //p' "$run/server.log" | jq -se --arg field "$field" --argjson expected "$expected" 'map(.[$field])==[$expected[0]]' >/dev/null
    "$ctl" checkpoints check > "$run/inventory-before.json"
    sha256sum "$run/checkpoint/CURRENT" > "$run/current-before.sha256"
    before_id=$(jq -r .checkpoint.last_success_id "$run/status.json")
    kill -9 "$pid"; wait "$pid" 2>/dev/null || true; pid=
    started=$(date +%s%N); start_server
    wait_status '.actual.status=="running" and .checkpoint.restored_from_checkpoint!=null'
    restored=$(date +%s%N)
    test "$(jq -r .checkpoint.state_generation "$run/status.json")" = "$generation"
    jq -e --argjson id "$before_id" '.checkpoint.restored_from_checkpoint==$id' "$run/status.json" >/dev/null
    sha256sum -c "$run/current-before.sha256" >/dev/null
    cp "$run/status.json" "$run/restored-status.json"
    for value in $append; do printf '{"device_id":"d1","v":%s}\n' "$value"; done >> "$run/events.jsonl"
    for _ in $(seq 1 100); do
        if sed -n 's/^sparrow-log //p' "$run/server.log" | jq -se --arg field "$field" --argjson expected "$expected" 'map(.[$field])==$expected' >/dev/null; then break; fi
        sleep .05
    done
    wait_status ".observation.runtime_progress.ingested_rows==$added"
    checkpoint; status
    jq -e --argjson before "$before_id" '.checkpoint.last_success_id>$before' "$run/status.json" >/dev/null
    "$ctl" stop check > "$run/stop.json"; wait_status '.actual.status=="stopped"'
    sed -n 's/^sparrow-log //p' "$run/server.log" | jq -se --arg field "$field" --argjson expected "$expected" 'map(.[$field])==$expected' >/dev/null
    "$ctl" checkpoints check > "$run/inventory-after.json"
    jq -e --argjson before "$before_id" --arg generation "$generation" '.storage.current>$before and all(.storage.generations[]; .state_generation==$generation and .snapshot_version==3 and .revision!=null and .attempt!=null)' "$run/inventory-after.json" >/dev/null
    jq -n --arg shape "$shape" --arg generation "$generation" --argjson expected "$expected" --argjson states "$states" \
        --argjson ms "$(((restored-started)/1000000))" \
        '{shape:$shape,states:$states,valid:true,expected_output:$expected,output_rows:2,duplicates:0,
          state_generation:$generation,generation_preserved:true,process_kill9_recovery:true,
          restore_to_running_ms:$ms,timing:"process_start_to_CLI_observed_running_includes_polling_not_kernel_only",scope:"isolated_local_not_power_loss_or_soak"}' > "$run/summary.json"
    kill -TERM "$pid"; wait "$pid"; pid=
done
compatibility=NOT_RUN
if [[ -n "$baseline" ]]; then
    test -x "$baseline"
    for direction in old-to-new new-to-old; do
        run="$root/$direction"; mkdir "$run"
        server="$baseline"; reader="$package/bin/sparrow-server"
        if [[ "$direction" == new-to-old ]]; then server="$package/bin/sparrow-server"; reader="$baseline"; fi
        catalog_path="$run/catalog.db"
        start_server
        printf '%s\n' '{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}' > "$run/stream.json"
        "$ctl" put-stream sensors "$run/stream.json" > "$run/put-stream.json"
        printf '%s\n' '{"device_id":"d1","v":1}' '{"device_id":"d1","v":2}' > "$run/events.jsonl"
        jq --arg path "$run/events.jsonl" --arg checkpoint "$run/checkpoint" \
            '.source.path=$path | .checkpoint_dir=$checkpoint | .checkpoint.interval_ms=null' \
            "$repo/deploy/pipeline-aligned.json" > "$run/spec.json"
        "$ctl" put-pipeline check "$run/spec.json" > "$run/put.json"
        "$ctl" start check > "$run/start.json"
        wait_status '.actual.status=="running" and .observation.runtime_progress.ingested_rows==2'
        checkpoint
        kill -TERM "$pid"; wait "$pid"; pid=
        sha256sum "$run/checkpoint/CURRENT" > "$run/current-before.sha256"
        server="$reader"
        if [[ "$direction" == new-to-old ]]; then
            # Do not hand the old reader the new binary's v3 catalog.  Let the
            # old binary create its own v2 catalog with the same stream and
            # pipeline spec, then point that reader at the new checkpoint.
            catalog_path="$run/reader-catalog.db"
            start_server
            "$ctl" put-stream sensors "$run/stream.json" > "$run/reader-put-stream.json"
            "$ctl" put-pipeline check "$run/spec.json" > "$run/reader-put.json"
            "$ctl" start check > "$run/reader-start.json"
        else
            catalog_path="$run/catalog.db"
            start_server
        fi
        pattern='legacy single-window'; if [[ "$direction" == new-to-old ]]; then pattern='snapshot version 3'; fi
        wait_status ".actual.status==\"failed\" and (.actual.last_error|test(\"$pattern\"))"
        cp "$run/status.json" "$run/incompatible-status.json"
        sha256sum -c "$run/current-before.sha256" >/dev/null
        sed -n 's/^sparrow-log //p' "$run/server.log" | jq -se 'length==0' >/dev/null
        if [[ "$direction" == old-to-new ]]; then
            # Fresh mode must not bypass the codec-directory guard. Preserve
            # CURRENT and every old chunk; do not "fix" rollback by deleting it.
            (cd "$run/checkpoint"; find . -type f ! -name WRITER_LOCK -exec sha256sum {} \; | sort) > "$run/history-before.sha256"
            jq '.checkpoint.resume_latest=false' "$run/spec.json" > "$run/fresh-spec.json"
            "$ctl" put-pipeline check "$run/fresh-spec.json" --if-match "$(jq -r .etag "$run/put.json")" > "$run/fresh-put.json"
            "$ctl" start check > "$run/fresh-start.json"
            wait_status '.actual.status=="failed" and (.actual.last_error|contains("refusing K1 writes"))'
            (cd "$run/checkpoint"; sha256sum -c "$run/history-before.sha256") > "$run/history-check.log"
            test ! -e "$run/checkpoint/STATE_GENERATION"
            curl --silent --show-error --fail -H "Authorization: Bearer $SPARROW_TOKEN" "$SPARROW_URL/v1/metrics" > "$run/guard-metrics.json"
            jq -e '.jobs_started==0 and .ingested_rows==0' "$run/guard-metrics.json" >/dev/null
        fi
        kill -TERM "$pid"; wait "$pid"; pid=
    done
    compatibility=PASS
fi
prototype_rollback=NOT_RUN
if [[ -n "$pre_r11" ]]; then
    test -x "$pre_r11"
    run="$root/k1-upgrade-rollback"; mkdir "$run"; server="$pre_r11"; catalog_path="$run/catalog.db"; start_server
    printf '%s\n' '{"fields":[{"name":"device_id","type":"utf8","nullable":false},{"name":"v","type":"int64","nullable":false}]}' > "$run/stream.json"
    "$ctl" put-stream sensors "$run/stream.json" > "$run/put-stream.json"
    printf '%s\n' '{"device_id":"d1","v":1}' '{"device_id":"d1","v":2}' > "$run/events.jsonl"
    jq --arg path "$run/events.jsonl" --arg checkpoint "$run/checkpoint" '.source.path=$path | .checkpoint_dir=$checkpoint | .checkpoint.interval_ms=null' "$repo/deploy/pipeline-aligned.json" > "$run/spec.json"
    "$ctl" put-pipeline check "$run/spec.json" > "$run/put.json"
    "$ctl" start check > "$run/start.json"
    wait_status '.actual.status=="running" and .observation.runtime_progress.ingested_rows==2'
    checkpoint; status; old_id=$(jq -r .checkpoint.last_success_id "$run/status.json")
    kill -TERM "$pid"; wait "$pid"; pid=
    # Preserve the stopped old writer's v2 catalog as the rollback input.
    # The upgrade intentionally uses the original path and may migrate it to
    # v3; rollback must never point the old binary at that migrated catalog.
    cp "$run/catalog.db" "$run/catalog-v2-backup.db"
    chmod 600 "$run/catalog-v2-backup.db"
    for suffix in -wal -shm; do
        if [[ -e "$run/catalog.db$suffix" ]]; then
            cp "$run/catalog.db$suffix" "$run/catalog-v2-backup.db$suffix"
            chmod 600 "$run/catalog-v2-backup.db$suffix"
        fi
    done
    sha256sum "$run/catalog-v2-backup.db" > "$run/catalog-v2-backup.sha256"
    catalog_path="$run/catalog.db"
    server="$package/bin/sparrow-server"; start_server
    wait_status ".actual.status==\"running\" and .checkpoint.restored_from_checkpoint==$old_id"
    printf '%s\n' '{"device_id":"d1","v":3}' >> "$run/events.jsonl"
    wait_status '.observation.runtime_progress.ingested_rows==1'
    checkpoint
    kill -TERM "$pid"; wait "$pid"; pid=
    test -f "$run/checkpoint/chk-$(printf '%08d' "$old_id")/PUBLISHED"
    sha256sum "$run/checkpoint/CURRENT" > "$run/current-new.sha256"
    server="$pre_r11"; catalog_path="$run/catalog-v2-backup.db"; start_server
    wait_status '.actual.status=="failed" and (.actual.last_error|contains("pipeline semantics changed"))'
    sha256sum -c "$run/current-new.sha256" >/dev/null
    sed -n 's/^sparrow-log //p' "$run/server.log" | jq -se 'length==1 and .[0].s==6' >/dev/null
    cp "$run/status.json" "$run/rollback-rejected.json"
    kill -TERM "$pid"; wait "$pid"; pid=
    prototype_rollback=PASS
fi
jq -s --arg compatibility "$compatibility" --arg prototype "$prototype_rollback" '{status:(if $compatibility=="PASS" then "PASS" else "PARTIAL" end),cases:.,bidirectional_old_codec_rejection:$compatibility,
    optional_historical_k1_upgrade_rollback:$prototype,
    scope:"File_zero_and_two_Count_real_process_recovery_not_exactly_once"}' "$root/zero/summary.json" "$root/two-count/summary.json" > "$root/summary.json"
if [[ "$compatibility" != PASS ]]; then printf 'K1_PROCESS_SMOKE_PARTIAL: R10 baseline required for full gate\n' >&2; exit 4; fi
printf 'K1_PROCESS_SMOKE_OK %s\n' "$root"

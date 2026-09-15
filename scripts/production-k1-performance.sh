#!/usr/bin/env bash
# Frozen driver + binaries only. New aligned shapes cannot be benchmarked as
# aligned on R10; compare fresh paths to R10 and periodic on/off within K1.
set -euo pipefail
art=${1:?new artifact directory}; driver=${2:?frozen K1 driver}; before=${3:?R10 server}; after=${4:?K1 server}
interval=${5:-100}; periodic_events=${6:-6400}
states_list=${K1_PERF_STATES:-"0 2"}; modes=${K1_PERF_MODES:-"fresh periodic"}
[[ "$interval" =~ ^[0-9]+$ && "$periodic_events" =~ ^[0-9]+$ ]]
test "$interval" -ge 100 && test "$interval" -le 86400000
[[ "$states_list" == 0 || "$states_list" == 2 || "$states_list" == '0 2' ]]
[[ "$modes" == fresh || "$modes" == periodic || "$modes" == 'fresh periodic' ]]
test ! -e "$art"; mkdir -p "$art"
for binary in "$driver" "$before" "$after"; do test -x "$binary"; done
jq -n --arg states "$states_list" --arg modes "$modes" --argjson interval "$interval" --argjson events "$periodic_events" \
    '{order:["before","after","after","before"],states:($states|split(" ")|map(tonumber)),modes:($modes|split(" ")),fresh_rounds:3,periodic_rounds:2,
    fresh_events:{zero:32000,two:131072},periodic_events:$events,periodic_interval_ms:$interval,
    fresh_throughput_min_ratio:0.97,periodic_min_ratio:0.90,rss_increase_max_kib:2048,
    correctness:"all_warmup_and_measured_valid_same_measured_hash",
    scope:"single_host_K1_File_not_WAN_or_power_loss",checkpoint_timing:"API_elapsed_includes_control_and_IO_not_compute_only"}' > "$art/plan.json"
sha256sum "$driver" "$before" "$after" > "$art/binaries.sha256"
summarize() {
    local prefix=$1 threshold=$2; shift 2
    jq -s --argjson threshold "$threshold" '
      def median:sort|length as $n|if $n%2==1 then .[$n/2|floor] else (.[($n/2)-1]+.[$n/2])/2 end;
      [.[]|select(.warmup==false)] as $r
      |([$r[]|select(.variant=="before")|.input_events_per_s]|median) as $b
      |([$r[]|select(.variant=="after")|.input_events_per_s]|median) as $a
      |([$r[]|select(.variant=="before")|.memory.sampled_peak_rss_kib]|max) as $br
      |([$r[]|select(.variant=="after")|.memory.sampled_peak_rss_kib]|max) as $ar
      |{all_valid:all(.valid),measured_trials:($r|length),output_hashes:([$r[].normalized_output_fnv1a64]|unique),
        before_events_per_s:$b,after_events_per_s:$a,ratio:($a/$b),rss_delta_kib:($ar-$br),
        checkpoint_successes:[$r[]|select(.variant=="after")|.periodic_checkpoint_status.succeeded_total],
        checkpoint_failures:[$r[]|select(.variant=="after")|.periodic_checkpoint_status.failed_or_cancelled_total],
        performance_target_met:($a/$b >= $threshold and $ar-$br<=2048)}' "$@" > "$art/$prefix-summary.json"
    jq -e '.all_valid and (.output_hashes|length)==1 and .performance_target_met' "$art/$prefix-summary.json" >/dev/null
}
failed=0
for states in $states_list; do
    window=1; events=32000
    if [[ "$states" == 2 ]]; then window=128; events=131072; fi
    for mode in $modes; do
        prefix="$mode-states-$states"; files=(); i=0
        for variant in before after after before; do
            i=$((i+1)); binary="$after"; extra=()
            if [[ "$mode" == fresh ]]; then
                if [[ "$variant" == before ]]; then binary="$before"; fi
                extra=(--scenarios file_count_window --file-events "$events" --rounds 3)
            else
                extra=(--scenarios file_chunked_no_checkpoint --checkpoint-events "$periodic_events" --rounds 2 --sink-delay-ms 20 --metrics-ms 1000)
                if [[ "$variant" == after ]]; then extra+=(--periodic-checkpoint-ms "$interval"); fi
            fi
            out="$art/$prefix-$i-$variant"
            "$driver" --engine sparrow --server-bin "$binary" --out "$out" --file-state-stages "$states" --window "$window" \
                --http-batch-rows 64 --http-linger-ms 5 --http-max-inflight 4 --drain-secs 30 "${extra[@]}" > "$out.log" 2>&1
            jq -es 'length>0 and all(.valid and .missing_rows==0 and .duplicate_rows==0 and .invalid_rows==0)' "$out/results.jsonl" >/dev/null
            jq -c --arg variant "$variant" '.+{variant:$variant}' "$out/results.jsonl" > "$out/labeled.jsonl"
            files+=("$out/labeled.jsonl")
        done
        threshold=0.97; if [[ "$mode" == periodic ]]; then threshold=0.90; fi
        if ! summarize "$prefix" "$threshold" "${files[@]}"; then
            if ! jq -e '.all_valid and (.output_hashes|length)==1' "$art/$prefix-summary.json" >/dev/null; then
                printf 'K1_CORRECTNESS_FAILURE %s\n' "$prefix" >&2; exit 2
            fi
            printf 'K1_PERFORMANCE_TARGET_NOT_MET %s; retaining all samples\n' "$prefix"
            failed=3
        fi
        if [[ "$mode" == periodic ]]; then
            jq -e 'all(.checkpoint_successes[];.>0) and all(.checkpoint_failures[];.==0)' "$art/$prefix-summary.json" >/dev/null
        fi
        if jq -e .performance_target_met "$art/$prefix-summary.json" >/dev/null; then printf 'K1_PERFORMANCE_OK %s\n' "$prefix"; fi
    done
done
exit "$failed"

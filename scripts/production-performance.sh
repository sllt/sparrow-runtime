#!/usr/bin/env bash
# No builds. The historical driver stays immutable for the R9 comparison;
# the new periodic driver is used only for the matched scheduler on/off trial.
set -euo pipefail
art=${1:?artifact directory}
before=${2:?R9 server binary}
test -x "$art/driver" && test -x "$art/periodic-driver" && test -x "$art/sparrow-server-production"
test ! -e "$art/periodic-plan.json"
jq -n '{scope:"single_host_matched_aligned_file_no_WAN_or_soak_claim",
    order:["off","on","on","off"],periodic_interval_ms:100,events:8000,rounds:2,
    window_rows:8,http_batch_rows:64,http_linger_ms:5,http_max_inflight:4,sink_delay_ms:20,
    correctness:"every warmup and measured trial valid; identical normalized output hashes within measured trials",
    timing:"first_append_to_last_sink; scheduler also runs during readiness/settle",
    throughput_ratio_min:0.90,rss_investigate_delta_kib:2048}' > "$art/periodic-plan.json"
bash scripts/obs-file-abba.sh "$art" "$before" 80000 2
bash scripts/obs-file-abba.sh "$art" "$before" 400000 3
files=(); i=0
for mode in off on on off; do
    i=$((i+1)); out="$art/periodic-$i-$mode"; extra=()
    if [[ "$mode" == on ]]; then extra=(--periodic-checkpoint-ms 100); fi
    "$art/periodic-driver" --engine sparrow --server-bin "$art/sparrow-server-production" --out "$out" \
        --scenarios file_chunked_no_checkpoint --checkpoint-events 8000 --window 8 --rounds 2 \
        --sink-delay-ms 20 --http-batch-rows 64 --http-linger-ms 5 --http-max-inflight 4 --metrics-ms 1000 \
        "${extra[@]}" > "$out.log" 2>&1
    jq -es 'length>0 and all(.valid and .missing_rows==0 and .duplicate_rows==0 and .invalid_rows==0)' "$out/results.jsonl" >/dev/null
    jq -c --arg mode "$mode" '.+{mode:$mode}' "$out/results.jsonl" > "$out/labeled.jsonl"
    files+=("$out/labeled.jsonl")
done
jq -s '
    def median: sort|length as $n|if $n%2==1 then .[$n/2|floor] else (.[($n/2)-1]+.[$n/2])/2 end;
    [.[]|select(.warmup==false)] as $r
    | ([$r[]|select(.mode=="off")|.input_events_per_s]|median) as $b
    | ([$r[]|select(.mode=="on")|.input_events_per_s]|median) as $a
    | ([$r[]|select(.mode=="off")|.memory.sampled_peak_rss_kib]|max) as $br
    | ([$r[]|select(.mode=="on")|.memory.sampled_peak_rss_kib]|max) as $ar
    | {all_valid:all(.valid),measured_trials:($r|length),output_hashes:([$r[].normalized_output_fnv1a64]|unique),
        off_median_events_per_s:$b,on_median_events_per_s:$a,ratio:($a/$b),rss_delta_kib:($ar-$br),
        checkpoint_successes:[$r[]|select(.mode=="on")|.periodic_checkpoint_status.succeeded_total],
        checkpoint_failures:[$r[]|select(.mode=="on")|.periodic_checkpoint_status.failed_or_cancelled_total],
        performance_target_met:($a/$b>=0.90 and $ar-$br<=2048)}' "${files[@]}" > "$art/periodic-summary.json"
jq -e '.all_valid and (.output_hashes|length)==1 and all(.checkpoint_successes[];.>0) and all(.checkpoint_failures[];.==0)' "$art/periodic-summary.json" >/dev/null
jq . "$art/periodic-summary.json"
jq -e .performance_target_met "$art/periodic-summary.json" >/dev/null

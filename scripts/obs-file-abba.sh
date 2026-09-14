#!/usr/bin/env bash
# No compilation. Preserve every round and compare immutable Server binaries.
# Usage: bash scripts/obs-file-abba.sh ARTIFACT_DIR BEFORE_SERVER [EVENTS] [ROUNDS]
set -euo pipefail
art=${1:?artifact directory with driver and sparrow-server-production}
before=${2:?immutable baseline server}
events=${3:-80000}
rounds=${4:-2}
case "$events:$rounds" in 80000:2|400000:3) ;; *) printf 'supported trials: 80000/2 or 400000/3\n' >&2; exit 2;; esac
driver="$art/driver"
after="$art/sparrow-server-production"
prefix="file-$events"
test -x "$driver" && test -x "$before" && test -x "$after"
test ! -e "$art/$prefix-plan.json"
jq -n --argjson events "$events" --argjson rounds "$rounds" \
    --arg before "$before" --arg after "$after" '{
    scope:"single_host_file_count_window_not_general_capacity_certification",
    order:["before","after","after","before"], events:$events, rounds:$rounds,
    warmup:"driver_uses_one_tenth_input", window_rows:800,
    before:$before, after:$after, throughput_target_ratio:0.97,
    rss_investigation_delta_kib:1024, timing:"submit_configuration_to_last_sink_arrival",
    extra_cpu_sampler:false, correctness:"all_rounds_valid_same_output_hash"
}' > "$art/$prefix-plan.json"
sha256sum "$driver" "$before" "$after" > "$art/$prefix-binaries.sha256"
files=()
i=0
for variant in before after after before; do
    i=$((i + 1))
    server="$after"; if [[ "$variant" == before ]]; then server="$before"; fi
    out="$art/$prefix-$i-$variant"
    test ! -e "$out"
    "$driver" --engine sparrow --server-bin "$server" --out "$out" \
        --scenarios file_count_window --file-events "$events" --window 800 \
        --rounds "$rounds" --metrics-ms 0 --observe-ms 0 > "$out.log" 2>&1
    jq -e -s 'length > 0 and all(.valid and .missing_rows == 0 and .duplicate_rows == 0 and .invalid_rows == 0)' \
        "$out/results.jsonl" > /dev/null
    jq -c --arg variant "$variant" '. + {variant:$variant}' "$out/results.jsonl" > "$out/labeled-results.jsonl"
    files+=("$out/labeled-results.jsonl")
    printf 'FILE_ABBA_ROUND_OK events=%s invocation=%s variant=%s\n' "$events" "$i" "$variant"
done
jq -s '
    def median: sort | length as $n | if $n % 2 == 1 then .[$n/2|floor] else (.[($n/2)-1]+.[$n/2])/2 end;
    [.[] | select(.warmup == false)] as $runs
    | [$runs[] | select(.variant == "before")] as $before
    | [$runs[] | select(.variant == "after")] as $after
    | ([$before[].input_events_per_s] | median) as $b
    | ([$after[].input_events_per_s] | median) as $a
    | ([$before[].memory.sampled_peak_rss_kib] | max) as $br
    | ([$after[].memory.sampled_peak_rss_kib] | max) as $ar
    | {measured_trials:($runs|length), all_valid:all(.valid),
       output_hashes:([$runs[].normalized_output_fnv1a64]|unique),
       before_median_events_per_s:$b, after_median_events_per_s:$a, ratio:($a/$b),
       before_sampled_peak_rss_kib:$br, after_sampled_peak_rss_kib:$ar, rss_delta_kib:($ar-$br),
       throughput_target_met:($a/$b >= 0.97), rss_investigation_required:($ar-$br > 1024)}
' "${files[@]}" > "$art/$prefix-summary.json"
jq -e '.all_valid and (.output_hashes|length)==1' "$art/$prefix-summary.json" > /dev/null
jq . "$art/$prefix-summary.json"
# Keep valid-but-slower trials as evidence, not a false correctness pass.
if ! jq -e '.throughput_target_met and (.rss_investigation_required|not)' "$art/$prefix-summary.json" > /dev/null; then
    printf 'PERFORMANCE_REVIEW_REQUIRED\n'; exit 3
fi

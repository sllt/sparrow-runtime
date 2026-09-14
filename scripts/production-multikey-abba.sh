#!/usr/bin/env bash
# No compilation. Explicitly compare the same multikey driver and workload.
# ART/driver-multikey, BEFORE and AFTER must be frozen binaries.
set -euo pipefail
art=${1:?artifact directory}; before=${2:?before server}; after=${3:?after server}
keys=${4:-1024}; events=${5:-131072}; window=${6:-128}; rounds=${7:-3}
[[ "$keys" =~ ^[0-9]+$ && "$events" =~ ^[0-9]+$ && "$window" =~ ^[0-9]+$ && "$rounds" =~ ^[1-9]$ ]]
test "$keys" -ge 1 && test "$keys" -le 1024
test "$window" -ge 8 && test "$window" -le 10000
test "$events" -ge "$((keys*window))" && test "$((events%(keys*window)))" -eq 0
test -x "$art/driver-multikey" && test -x "$before" && test -x "$after"
prefix="multikey-$keys-$events"
test ! -e "$art/$prefix-plan.json"
jq -n --arg before "$before" --arg after "$after" --argjson keys "$keys" --argjson events "$events" --argjson window "$window" --argjson rounds "$rounds" \
  '{scope:"single_host_interleaved_keys_MIN_MAX_SUM_not_WAN_or_soak",before:$before,after:$after,keys:$keys,events:$events,window:$window,rounds:$rounds,
    order:["before","after","after","before"],warmup:"one_tenth_rounded_to_complete_keyed_windows_minimum_one_cycle",
    unchanged_extrema:"all_but_first_two_rows_per_keyed_window",unused_input_string_bytes:256,
    throughput_target_ratio:0.97,rss_investigation_delta_kib:2048,
    correctness:"every_trial_valid_identical_measured_output_hashes",timing:"configuration_submit_to_last_capture_arrival"}' > "$art/$prefix-plan.json"
sha256sum "$art/driver-multikey" "$before" "$after" > "$art/$prefix-binaries.sha256"
files=(); i=0
for variant in before after after before; do
    i=$((i+1)); server="$after"; if [[ "$variant" == before ]]; then server="$before"; fi
    out="$art/$prefix-$i-$variant"
    "$art/driver-multikey" --engine sparrow --server-bin "$server" --out "$out" --scenarios file_multikey_minmax \
        --file-keys "$keys" --file-events "$events" --window "$window" --rounds "$rounds" --drain-secs 30 > "$out.log" 2>&1
    jq -es 'length>0 and all(.valid and .missing_rows==0 and .duplicate_rows==0 and .invalid_rows==0)' "$out/results.jsonl" >/dev/null
    jq -c --arg variant "$variant" '.+{variant:$variant}' "$out/results.jsonl" > "$out/labeled.jsonl"
    files+=("$out/labeled.jsonl")
done
jq -s '
    def median:sort|length as $n|if $n%2==1 then .[$n/2|floor] else (.[($n/2)-1]+.[$n/2])/2 end;
    [.[]|select(.warmup==false)] as $r
    |([$r[]|select(.variant=="before")|.input_events_per_s]|median) as $b
    |([$r[]|select(.variant=="after")|.input_events_per_s]|median) as $a
    |([$r[]|select(.variant=="before")|.memory.sampled_peak_rss_kib]|max) as $br
    |([$r[]|select(.variant=="after")|.memory.sampled_peak_rss_kib]|max) as $ar
    |{all_valid:all(.valid),measured_trials:($r|length),output_hashes:([$r[].normalized_output_fnv1a64]|unique),
      before_median_events_per_s:$b,after_median_events_per_s:$a,ratio:($a/$b),rss_delta_kib:($ar-$br),
      performance_target_met:($a/$b>=0.97 and $ar-$br<=2048)}' "${files[@]}" > "$art/$prefix-summary.json"
jq -e '.all_valid and (.output_hashes|length)==1 and .performance_target_met' "$art/$prefix-summary.json" >/dev/null
printf 'MULTIKEY_ABBA_OK %s\n' "$prefix"

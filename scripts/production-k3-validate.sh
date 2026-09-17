#!/usr/bin/env bash
# No compilation. Repeat exact frozen tests, then fault the matching server
# process on private loopback fixtures. Not TLS/WAN/target-device/soak approval.
set -euo pipefail
art=${1:?new artifact directory}; package=${2:?production package}; frozen=${3:?frozen tests}; driver=${4:?compiled K3 process driver}; rounds=${5:-20}
profile=${SPARROW_K3_TEST_PROFILE:-core}; case "$profile" in core|reliable) ;; *) exit 2;; esac
[[ "$rounds" =~ ^[1-9][0-9]?$ ]]
test ! -e "$art"; mkdir -p "$art"; art=$(cd "$art" && pwd)
package=$(cd "$package" && pwd); frozen=$(cd "$frozen" && pwd)
test -x "$driver"; test -x "$package/bin/sparrow-server"
(cd "$package" && sha256sum -c SHA256SUMS) > "$art/package-verify.log"
(cd "$frozen" && sha256sum -c "$profile-test-binaries.sha256") > "$art/frozen-verify.log"
bins=(); selected=0
while IFS= read -r file_path; do
    binary="$frozen/$profile-test-binaries/$(basename "$file_path")"
    "$binary" --list k3_ > "$art/$(basename "$binary").selected.txt"
    count=$(grep -c ': test$' "$art/$(basename "$binary").selected.txt" || true)
    if [[ "$count" -gt 0 ]]; then bins+=("$binary"); selected=$((selected+count)); fi
done < <(jq -r '.[].exe' "$frozen/$profile-test-binaries.json")
[[ "$selected" -ge 16 ]]
jq -n --argjson rounds "$rounds" --argjson count "$selected" '{rounds:$rounds,tests_per_round:$count,scope:"frozen_deterministic_repeat_not_soak",filter:"k3_"}' > "$art/repeat-plan.json"
for ((round=1;round<=rounds;round++)); do
    for binary in "${bins[@]}"; do
        printf 'ROUND=%s BINARY=%s\n' "$round" "$binary" >> "$art/repeat.log"
        "$binary" k3_ --test-threads=1 >> "$art/repeat.log" 2>&1
    done
done
sha256sum "$driver" "$package/bin/sparrow-server" > "$art/binaries.sha256"
"$driver" --server-bin "$package/bin/sparrow-server" --out "$art/process" > "$art/process.log" 2>&1
printf 'K3_VALIDATE_OK rounds=%s tests_per_round=%s total=%s; zero/two-state multi-source process faults passed (not production certification)\n' "$rounds" "$selected" "$((rounds*selected))"

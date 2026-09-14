#!/usr/bin/env bash
# Run one feature profile once, then freeze its exact test executables.
# Usage: bash scripts/production-freeze-tests.sh ARTIFACT_DIR core|production
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
art=${1:?artifact directory required}; profile=${2:?core or production required}
case "$profile" in core|production) ;; *) exit 2;; esac
command -v cargo >/dev/null; command -v jq >/dev/null; command -v sha256sum >/dev/null
mkdir -p "$art"; art=$(cd "$art" && pwd)
test ! -e "$art/$profile-test-binaries.json"
test ! -e "$art/$profile-messages.jsonl"
extra=(); if [[ "$profile" == production ]]; then extra=(--no-default-features -p sparrow-server -p sparrow-cli); fi
if ! cargo test --locked --release --quiet --message-format=json "${extra[@]}" \
    > "$art/$profile-messages.jsonl" 2> "$art/$profile-build.log"; then
    printf 'TEST_PROFILE_FAILED %s; logs: %s\n' "$profile" "$art" >&2
    tail -n 40 "$art/$profile-build.log" >&2
    grep -A 12 -B 2 -E 'FAILED|panicked|test result: FAILED' "$art/$profile-messages.jsonl" >&2 || true
    exit 1
fi
jq -Rn '[inputs|fromjson?|select(.reason=="compiler-artifact" and .profile.test and .executable!=null)
    |{name:.target.name,exe:.executable}]' "$art/$profile-messages.jsonl" > "$art/$profile-test-binaries.json"
jq -e 'length>0' "$art/$profile-test-binaries.json" >/dev/null
mkdir "$art/$profile-test-binaries"
while IFS= read -r file_path; do
    test -x "$file_path"
    cp "$file_path" "$art/$profile-test-binaries/$(basename "$file_path")"
done < <(jq -r '.[].exe' "$art/$profile-test-binaries.json")
(cd "$art"; find "$profile-test-binaries" -type f -print | LC_ALL=C sort |
    while IFS= read -r file_path; do sha256sum "$file_path"; done) > "$art/$profile-test-binaries.sha256"
printf 'FROZEN_TEST_PROFILE_OK %s %s\n' "$profile" "$art"

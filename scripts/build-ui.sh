#!/usr/bin/env bash
# Build the optional ops workbench UI into a NEW directory (K5.6).
# Usage: bash scripts/build-ui.sh NEW_UI_DIRECTORY
# Requires the pinned Node/npm (web/.nvmrc, web/package.json engines). Set
# SPARROW_UI_OFFLINE=1 to install only from a pre-populated npm cache
# (no network); the build itself never fetches remote assets.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out=${1:?new ui directory required}
[[ ! -e "$out" ]] || { printf 'ui output must not exist: %s\n' "$out" >&2; exit 2; }
case "$(cd "$(dirname "$out")" && pwd)/" in "$root/web/"*) printf 'ui output must be outside web/\n' >&2; exit 2;; esac
cd "$root/web"
want_node=$(tr -d 'v \n' < .nvmrc)
have_node=$(node --version | tr -d 'v')
[[ "$have_node" == "$want_node" ]] || { printf 'node %s required, found %s\n' "$want_node" "$have_node" >&2; exit 2; }
npm_args=(ci --no-audit --no-fund)
if [[ "${SPARROW_UI_OFFLINE:-0}" == 1 ]]; then npm_args+=(--offline); fi
npm "${npm_args[@]}" >/dev/null
rm -rf dist
npm run build >/dev/null
# The server refuses a UI dir without index.html; fail here first.
test -f dist/index.html
# No remote asset references may ship (CSP is self-only anyway).
if grep -rEl '(src|href)="https?://' dist >/dev/null; then printf 'dist references remote assets\n' >&2; exit 1; fi
mkdir -p "$out"
cp -R dist/. "$out/"
fingerprint=$( (sha256sum package.json package-lock.json .nvmrc index.html vite.config.ts tsconfig*.json; find src -type f ! -name '*.test.ts' -print | LC_ALL=C sort | xargs sha256sum) | sha256sum | cut -d' ' -f1)
contract=$(sed -n 's/^export const UI_CONTRACT = \([0-9]*\);/\1/p' src/auth/AuthContext.tsx)
server_contract=$(sed -n 's/^pub const UI_CONTRACT: u32 = \([0-9]*\);/\1/p' "$root/crates/sparrow-server/src/ui.rs")
[[ -n "$contract" && "$contract" == "$server_contract" ]] || { printf 'UI contract %s != server %s\n' "$contract" "$server_contract" >&2; exit 1; }
jq -n --arg fp "$fingerprint" --arg node "$have_node" --arg npm "$(npm --version)" --argjson contract "$contract" \
    '{format:"sparrow-ui-build-v1",source_fingerprint_sha256:$fp,node:$node,npm:$npm,ui_contract:$contract,serve:"sparrow-server --ui-dir <this directory>"}' > "$out/ui-build.json"
(cd "$out" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | while IFS= read -r f; do sha256sum "$f"; done) > "$out/SHA256SUMS"
printf 'UI_BUILD_OK %s\n' "$out"

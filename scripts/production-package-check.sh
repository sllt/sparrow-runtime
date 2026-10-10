#!/usr/bin/env bash
# Inspect a built package and its real feature inventory; never build/deploy.
set -euo pipefail
package=$(cd "${1:?package directory}" && pwd)
root=${2:?new evidence directory}
for tool in jq curl sha256sum ss; do command -v "$tool" >/dev/null; done
test ! -e "$root"; mkdir -p "$root"; root=$(cd "$root" && pwd)
(cd "$package" && sha256sum --strict -c SHA256SUMS) > "$root/checksums.log"
for path in README.md docs/PRODUCTION.md docs/RUNTIME.md docs/CONNECTORS.md docs/FORMATS.md \
    docs/DURABLE_OUTPUT.md docs/RECOVERY_OPERATIONS.md docs/ANALYSIS.md docs/REFERENCE_TABLES.md \
    docs/NATS.md docs/WEBSOCKET.md docs/POSTGRES.md deploy/pipeline-durable-http.json; do
    test -s "$package/$path"
done
test ! -e "$package/docs/DEVELOPMENT_ORDER.md"
test ! -e "$package/docs/DEVELOPMENT_TODO.md"
grep -q '  README.md$' "$package/evidence/source-files.sha256"
grep -q '  docs/RECOVERY_OPERATIONS.md$' "$package/evidence/source-files.sha256"
jq -e '.default_features==false and .target=="x86_64-unknown-linux-gnu"
    and (.source_commit|test("^[0-9a-f]{40}$"))
    and ([.nats_enabled,.jetstream_enabled,.websocket_enabled,.postgres_enabled]|all(type=="boolean"))
    and ((.jetstream_enabled|not) or .nats_enabled)' "$package/build.json" >/dev/null
port=${SPARROW_SMOKE_PORT:-$((21000 + $$%8000))}
test -z "$(ss -H -ltn "sport = :$port")"
export SPARROW_TOKEN=package-check-not-a-deployment-secret
export SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef SPARROW_REQUIRE_SECRETS_KEY=1
export SPARROW_DATA_ROOTS="$root"
pid=
cleanup() { if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; }
trap cleanup EXIT
"$package/bin/sparrow-server" --safe-mode --bind "127.0.0.1:$port" --catalog "$root/catalog.db" > "$root/server.log" 2>&1 & pid=$!
for _ in $(seq 1 100); do
    if curl --silent --fail -H "Authorization: Bearer $SPARROW_TOKEN" "http://127.0.0.1:$port/v1/capabilities" > "$root/capabilities.json"; then break; fi
    kill -0 "$pid"; sleep .05
done
jq -e --slurpfile build "$package/build.json" '
    .inventory.combinations as $c | $build[0] as $b |
    all(["nats","jetstream","websocket","postgres"][];
        . as $name | ($c|any(.source==$name or .sink==$name)) == $b[$name+"_enabled"])
' "$root/capabilities.json" >/dev/null
"$package/bin/sparrow-server" --version > "$root/server-version.txt"
"$package/bin/sparrowctl" --version > "$root/cli-version.txt"
cmp "$root/server-version.txt" "$package/evidence/server-version.txt"
cmp "$root/cli-version.txt" "$package/evidence/cli-version.txt"
kill -TERM "$pid"; wait "$pid"; pid=
printf 'PRODUCTION_PACKAGE_CHECK_OK %s\n' "$root"

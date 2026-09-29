#!/usr/bin/env bash
set -euo pipefail
export SPARROW_PLUGIN_SMOKE_KIND=wasm
exec bash "$(dirname "$0")/plugins-native-smoke.sh" "$@"

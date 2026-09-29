#!/usr/bin/env bash
# Same authenticated Server/CLI lifecycle contract, with no native-code opt-in.
set -euo pipefail
export SPARROW_PLUGIN_SMOKE_KIND=script
exec bash "$(dirname "$0")/plugins-native-smoke.sh" "$@"

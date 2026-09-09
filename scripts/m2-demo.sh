#!/usr/bin/env bash
# Start the in-process MQTT broker + HTTP capture + Sparrow pipeline.
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo run -p sparrow-cli --bin m2_mqtt_http_loop "$@"

#!/usr/bin/env bash
# Fetch the pinned, checksum-verified Apache Kafka (KRaft) and Temurin JRE
# used by the opt-in Kafka broker tests, then print the environment for
#   cargo test -p sparrow-connectors --features kafka -- --ignored kafka
# Usage: scripts/kafka-broker.sh [dest-dir]   (default: target/kafka-broker)
set -euo pipefail

KAFKA_VERSION=4.3.1
KAFKA_TGZ="kafka_2.13-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
# Published at ${KAFKA_URL}.sha512
KAFKA_SHA512=c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c

JRE_TGZ="OpenJDK21U-jre_x64_linux_hotspot_21.0.12.1_1.tar.gz"
JRE_URL="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12.1%2B1/${JRE_TGZ}"
# Published at ${JRE_URL}.sha256.txt
JRE_SHA256=2413149700df0f7d440500a84a8f764c535f21e5a5e87d38328b64eec2c5b500

if [[ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]]; then
  echo "kafka-broker.sh: pinned JRE is linux x86_64 only" >&2
  exit 1
fi

dest="${1:-target/kafka-broker}"
mkdir -p "$dest"
cd "$dest"

fetch() { # url file algo sum
  if [[ ! -f "$2" ]]; then
    curl -fsSL --retry 3 -o "$2.part" "$1"
    mv "$2.part" "$2"
  fi
  echo "$4  $2" | "$3sum" -c --quiet - || { rm -f "$2"; echo "checksum mismatch: $2" >&2; exit 1; }
}

fetch "$KAFKA_URL" "$KAFKA_TGZ" sha512 "$KAFKA_SHA512"
fetch "$JRE_URL" "$JRE_TGZ" sha256 "$JRE_SHA256"

[[ -d "kafka_2.13-${KAFKA_VERSION}" ]] || tar xzf "$KAFKA_TGZ"
[[ -d jre ]] || { mkdir jre && tar xzf "$JRE_TGZ" -C jre --strip-components=1; }

echo "export SPARROW_KAFKA_HOME=$(pwd)/kafka_2.13-${KAFKA_VERSION}"
echo "export SPARROW_JAVA_HOME=$(pwd)/jre"

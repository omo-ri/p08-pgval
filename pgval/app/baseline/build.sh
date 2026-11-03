#!/usr/bin/env bash
# Build the author's Flink job (SST/MST) from DSPF-BM at the pinned commit.
# The author's Dockerfile needs a settings.xml that is not in the repo and pulls
# datamodel from authenticated GitHub Packages, so we install datamodel into a
# local Maven cache first and then package flink-topology.
set -euo pipefail

DSPF_BM=${DSPF_BM:-$HOME/repos/DSPF-BM}
PINNED=d74c9b80a3a1
OUT=$(cd "$(dirname "$0")/.." && pwd)/target

actual=$(git -C "$DSPF_BM" rev-parse HEAD)
if [[ $actual != "$PINNED"* ]]; then
  echo "DSPF-BM is at $actual, expected $PINNED" >&2
  exit 1
fi

mkdir -p "$OUT"
docker run --rm \
  -v "$DSPF_BM/docker:/src:ro" \
  -v p08-m2:/root/.m2 \
  -v "$OUT:/out" \
  maven:3.9.6-eclipse-temurin-11 \
  bash -c '
    set -e
    cp -r /src/datamodel /src/flink-topology /tmp/
    mvn -q -B -f /tmp/datamodel/pom.xml install -DskipTests
    mvn -q -B -f /tmp/flink-topology/pom.xml package -DskipTests
    cp /tmp/flink-topology/target/flink-topology-1.0-SNAPSHOT.jar /out/topology.jar
    chown '"$(id -u):$(id -g)"' /out/topology.jar
  '
sha256sum "$OUT/topology.jar"

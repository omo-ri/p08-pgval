#!/usr/bin/env bash
# Build the team's Flink job (pgval/app/job) -> pgval/app/target/job.jar.
# Maven runs in Docker, so only Docker is needed on the host.
set -euo pipefail

JOB=$(cd "$(dirname "$0")" && pwd)
OUT=$(cd "$JOB/.." && pwd)/target

mkdir -p "$OUT"
docker run --rm \
  -v "$JOB:/src:ro" \
  -v p08-m2:/root/.m2 \
  -v "$OUT:/out" \
  maven:3.9.6-eclipse-temurin-11 \
  bash -c '
    set -e
    cp -r /src /tmp/job
    mvn -q -B -f /tmp/job/pom.xml package
    cp /tmp/job/target/p08-job-0.1.jar /out/job.jar
    chown '"$(id -u):$(id -g)"' /out/job.jar
  '
sha256sum "$OUT/job.jar"

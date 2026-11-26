#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
demo=pgval/executor/demo
out="${1:-runs/executor-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
mkdir -p "$(dirname "$out")"
mkdir "$out"

{
    uname -srmo
    python3 --version
    java -version
} > "$out/environment.txt" 2>&1

mkdir -p "$demo/build/classes"
compile='javac --release 11 -d build/classes Stub.java &&
         jar --create --file build/stub.jar --main-class Stub -C build/classes Stub.class'
image=maven:3.9.9-eclipse-temurin-11
{
    if command -v javac >/dev/null && command -v jar >/dev/null; then
        (cd "$demo"; javac -version; bash -c "$compile")
    elif command -v docker.exe >/dev/null && command -v wslpath >/dev/null; then
        docker.exe run --rm --label pgval.component=executor-demo \
            --mount "type=bind,source=$(wslpath -w "$(realpath "$demo")"),target=/demo" \
            -w /demo "$image" bash -ec "javac -version && $compile"
    else
        docker run --rm --label pgval.component=executor-demo --user "$(id -u):$(id -g)" \
            --mount "type=bind,source=$(realpath "$demo"),target=/demo" \
            -w /demo "$image" bash -ec "javac -version && $compile"
    fi
    sha256sum "$demo/build/stub.jar"
} 2>&1 | tee "$out/build.txt"

python3 -m pgval.executor run --scenario "$demo/scenarios/local-normal.json" --out "$out/c0" \
    | tee "$out/c0.txt"
python3 -m pgval.executor run --scenario "$demo/scenarios/local-restart.json" --out "$out/c1" \
    | tee "$out/c1.txt"
python3 "$demo/check_failures.py" --out "$out/negative" --restart-run "$out/c1" \
    | tee "$out/negative.txt"
printf 'Executor demo passed. Results: %s\n' "$out"

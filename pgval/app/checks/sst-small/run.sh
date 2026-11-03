#!/usr/bin/env bash
# Small SST check with a known answer: fresh stand -> author's SST job (E1) ->
# hand-written events -> read_committed output -> compare with expected-*.jsonl.
# Results go to results/<timestamp>/.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
APP=$(cd "$HERE/../.." && pwd)
OUT=$HERE/results/$(date +%Y%m%dT%H%M%S)
mkdir -p "$OUT"
cd "$APP"

docker compose down -v >/dev/null 2>&1
PG=e1 docker compose up -d --wait >/dev/null 2>&1
for _ in $(seq 60); do
  curl -s localhost:8081/jobs/overview | grep -q '"state":"RUNNING"' && break
  sleep 1
done
docker compose logs submit > "$OUT/submit.log" 2>&1
sleep 10  # first checkpoints done

produce() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic click < "$1"
}
consume() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$1" --from-beginning \
    --isolation-level read_committed --timeout-ms 8000 2>/dev/null
}

# Watermarks are emitted every 200 ms, not per event: event 11 is late only if the
# watermark has already passed 10:01:00 when it arrives, so the batches are separated.
produce "$HERE/input-1.jsonl"
sleep 3
produce "$HERE/input-2.jsonl"
sleep 12  # E1 output becomes visible after the next checkpoint (5 s)

consume output > "$OUT/output.jsonl"
consume lateOutput > "$OUT/lateOutput.jsonl"
curl -s localhost:8081/jobs/overview > "$OUT/jobs.json"
docker compose logs jobmanager taskmanager > "$OUT/flink.log" 2>&1

python3 -I - "$HERE" "$OUT" <<'EOF' | tee "$OUT/compare.txt"
import json, sys
here, out = sys.argv[1:]
load = lambda p: [json.loads(l) for l in open(p) if l.strip()]
win = lambda r: (r["page"], r["windowStart"], r["windowEnd"], tuple(r["clickIds"]))
late = lambda r: (r["page"], r["clickId"])
ok = True
for name, key in (("output", win), ("late", late)):
    exp = sorted(map(key, load(f"{here}/expected-{name}.jsonl")))
    act = sorted(map(key, load(f"{out}/{'output' if name == 'output' else 'lateOutput'}.jsonl")))
    missing = [r for r in exp if r not in act]
    extra = [r for r in act if r not in exp or act.count(r) > exp.count(r)]
    print(f"{name}: expected {len(exp)}, actual {len(act)}, missing {missing}, extra {extra}")
    ok &= not missing and not extra
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
EOF

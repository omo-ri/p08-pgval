#!/usr/bin/env bash
# SST check with generated load: fresh stand -> author's SST job (E1) -> fakegen events
# -> read_committed output -> compare with an expected answer computed from the input.
# Both fakegen and the comparison below are temporary stand-ins for the team's
# generator and validator. Extra arguments go to gen.py. Results: results/<timestamp>/.
#
# Environment:
#   PG=e1|a1        processing guarantee of the job (default e1)
#   KILL_AFTER=N    docker kill the TaskManager N s after generation starts (default: no fault)
#   DOWN_FOR=M      start it again M s later (default 5)
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
APP=$(cd "$HERE/../.." && pwd)
OUT=$HERE/results/$(date +%Y%m%dT%H%M%S)
PG=${PG:-e1}
mkdir -p "$OUT"
cd "$APP"

docker compose down -v >/dev/null 2>&1
PG=$PG docker compose up -d --wait >/dev/null 2>&1
for _ in $(seq 60); do
  curl -s localhost:8081/jobs/overview | grep -q '"state":"RUNNING"' && break
  sleep 1
done
sleep 10  # first checkpoints done

echo "PG=$PG KILL_AFTER=${KILL_AFTER:-} DOWN_FOR=${DOWN_FOR:-5} gen.py $*" > "$OUT/args.txt"
stamp() { echo "$(date +%T.%3N) $*" >> "$OUT/timeline.txt"; }
stamp "generation start"
if [[ -n ${KILL_AFTER:-} ]]; then
  (
    sleep "$KILL_AFTER"; stamp "kill taskmanager"; docker compose kill taskmanager >/dev/null 2>&1
    sleep "${DOWN_FOR:-5}"; stamp "start taskmanager"; docker compose start taskmanager >/dev/null 2>&1
  ) &
fi
python3 "$APP/fakegen/gen.py" "$@" | tee "$OUT/input.tsv" |
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic click \
    --property parse.key=true --property key.separator=$'\t'
stamp "generation end"
wait
sleep 20  # recovery, then E1 output becomes visible after the next checkpoint (5 s)

for topic in output lateOutput; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$topic" --from-beginning \
    --isolation-level read_committed --timeout-ms 8000 2>/dev/null > "$OUT/$topic.jsonl"
done
docker compose logs jobmanager taskmanager > "$OUT/flink.log" 2>&1
grep -E 'Job .* switched from state|Restoring job .* from Checkpoint' "$OUT/flink.log" \
  | sed -E 's/^jobmanager-1 +\| //' >> "$OUT/timeline.txt" || true

python3 -I - "$OUT" <<'EOF' | tee "$OUT/compare.txt"
import json, sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
out = sys.argv[1]
W = 60_000

def ms(s):
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)

events = [json.loads(l.split("\t", 1)[1]) for l in open(f"{out}/input.tsv")]
watermark = max(ms(e["timestamp"]) for e in events) - 1
expected = defaultdict(set)
for e in events:
    start = ms(e["timestamp"]) // W * W
    if start + W - 1 <= watermark:  # only windows the final watermark has closed
        expected[(e["page"], start)].add(e["id"])

seen = Counter()
incorrect = 0
for line in open(f"{out}/output.jsonl"):
    r = json.loads(line)
    key = (r["page"], r["windowStart"])
    seen[key] += 1
    if key in expected and set(r["clickIds"]) != expected[key]:
        incorrect += 1
        print(f"incorrect {key}: missing {sorted(expected[key] - set(r['clickIds']))[:10]}, "
              f"extra {sorted(set(r['clickIds']) - expected[key])[:10]}")

missing = [k for k in expected if seen[k] == 0]
duplicate = {k: c for k, c in seen.items() if c > 1}
unexpected = [k for k in seen if k not in expected]
late = sum(1 for _ in open(f"{out}/lateOutput.jsonl"))
print(f"input events {len(events)}, expected windows {len(expected)}, output records {sum(seen.values())}")
print(f"missing windows {len(missing)}, duplicate windows {len(duplicate)}, "
      f"incorrect windows {incorrect}, unexpected windows {len(unexpected)}, late events {late}")
ok = not (missing or duplicate or incorrect or unexpected or late)
print("PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
EOF

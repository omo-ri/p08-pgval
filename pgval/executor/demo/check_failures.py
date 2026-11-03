"""Exercise failure handling against small, real child processes."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


TARGET = """
import json
import sys
import time

case = sys.argv[1]
if case == "ready_timeout":
    time.sleep(60)
print('{"type":"ready"}', flush=True)
for line in sys.stdin:
    row = json.loads(line)
    if row["type"] == "finish":
        if case == "duplicate":
            print('{"type":"result","event_id":"e1","result":4}', flush=True)
        break
    ident = row["event_id"]
    if case == "unexpected_exit":
        sys.exit(12)
    if case == "bad_json":
        print('{', flush=True)
        break
    if case == "trigger_not_reached" and ident == "e2":
        time.sleep(60)
    if case == "wrong_id":
        ident = "unknown"
    print(json.dumps({"type": "result", "event_id": ident, "result": row["value"] * 2}), flush=True)
"""


def gone(pid):
    return not Path(f"/proc/{pid}").exists()


def journal(out):
    return [json.loads(line) for line in (out / "journal.jsonl").read_text().splitlines()]


def run_case(root, case, reason):
    kind = "restart" if case == "trigger_not_reached" else "normal"
    config = json.loads(Path(f"pgval/executor/demo/scenarios/local-{kind}.json").read_text())
    config["command"] = [sys.executable, "-c", TARGET, case]
    config["timeouts_s"].update(ready=0.6, stage=0.6)
    scenario = root / f"{case}.json"
    scenario.write_text(json.dumps(config, indent=2) + "\n")
    out = root / case
    result = subprocess.run([sys.executable, "-m", "pgval.executor", "run", "--scenario", str(scenario),
                             "--out", str(out)], capture_output=True, timeout=20)
    (out / "cli.stdout").write_bytes(result.stdout)
    (out / "cli.stderr").write_bytes(result.stderr)
    summary = json.loads((out / "summary.json").read_text())
    events = journal(out)
    pids = [event["pid"] for event in events if event["event_type"] == "process_started"]
    assert pids and all(map(gone, pids)), pids
    assert result.returncode == 1 and summary["reason_code"] == reason, summary
    if case == "duplicate":
        assert summary["execution_status"] == "completed", summary
        assert summary["validation_status"] == "failed" and summary["extra"] == 1, summary
    else:
        assert summary["execution_status"] == "failed", summary
    if case in ("ready_timeout", "trigger_not_reached"):
        assert any(event["event_type"] == "cleanup_exit" and event["returncode"] == -9 for event in events)
    if case == "unexpected_exit":
        assert any(event["event_type"] == "process_exit" and event["returncode"] == 12 for event in events)
    print(f"OK {case}, PID cleaned up")


def confirm_restart(out):
    events = journal(out)
    sequence = [event["event_type"] for event in events]
    summary = json.loads((out / "summary.json").read_text())
    starts = [event for event in events if event["event_type"] == "process_started"]
    exits = [event for event in events if event["event_type"] == "process_exit"]
    assert summary["validation_status"] == "passed" and summary["confirmed_faults"] == 1, summary
    assert len(starts) == 2 and starts[0]["pid"] != starts[1]["pid"], starts
    assert all(gone(event["pid"]) for event in starts), starts
    assert [event["returncode"] for event in exits] == [-9, 0], exits
    assert [event["attempt"] for event in events if event["event_type"] == "stage"] == [1, 2]
    assert sequence.count("fault_sent") == 1 and sequence.count("fault_confirmed") == 1
    fault = json.loads((out / "scenario.json").read_text())["fault"]
    effects = [event for event in events if event["event_type"] in ("fault_sent", "fault_confirmed")]
    assert all(event["action_id"] == fault["action_id"]
               and event["event_id"] == fault["event_id"]
               and event["pid"] == starts[0]["pid"] for event in effects), effects
    stages = [event["seq"] for event in events if event["event_type"] == "stage"]
    fault_sent = next(event["seq"] for event in events if event["event_type"] == "fault_sent")
    confirmed = next(event["seq"] for event in events if event["event_type"] == "fault_confirmed")
    ready = next(event["seq"] for event in events if event["event_type"] == "ready" and event["attempt"] == 2)
    replay = next(event["seq"] for event in events if event["event_type"] == "event_sent"
                  and event["attempt"] == 2 and event["event_id"] == "e2")
    release = next(event["seq"] for event in events if event["event_type"] == "message_sent"
                    and event["message"] == {"type": "continue", "event_id": "e2"})
    result = next(event["seq"] for event in events if event["event_type"] == "result"
                  and event["event_id"] == "e2")
    assert stages[0] < fault_sent < exits[0]["seq"] < confirmed < ready < replay < stages[1] < release < result
    assert '"event_id":"e2","result"' not in (out / "stdout-1.log").read_text()
    assert '"event_id":"e2","result":6' in (out / "stdout-2.log").read_text()
    print("OK C1: two barriers, one SIGKILL and reaped JVMs")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--restart-run", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    for case, reason in (
        ("ready_timeout", "ready_timeout"),
        ("trigger_not_reached", "trigger_not_reached"),
        ("unexpected_exit", "unexpected_exit"),
        ("bad_json", "protocol_error"),
        ("wrong_id", "protocol_error"),
        ("duplicate", None),
    ):
        run_case(args.out, case, reason)
    confirm_restart(args.restart_run)


if __name__ == "__main__":
    main()

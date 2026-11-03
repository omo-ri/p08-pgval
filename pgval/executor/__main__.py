import argparse
import hashlib
import json
import math
import os
import platform
import re
import select
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .fixture_check import compare


class RunError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def utc():
    return datetime.now(timezone.utc).isoformat()


def encode(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"


def save(path, row):
    path.write_text(json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def decode(line):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key}")
            result[key] = value
        return result

    row = json.loads(line.decode("utf-8"), object_pairs_hook=unique)
    if not isinstance(row, dict):
        raise ValueError("expected a JSON object")
    return row


def event_id(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value)


def read_records(path, field):
    rows = [decode(line) for line in path.read_bytes().splitlines()]
    ids = set()
    for row in rows:
        if set(row) != {"event_id", field} or not event_id(row["event_id"]):
            raise ValueError(f"{path}: invalid record {row}")
        limit = 2**31 if field == "value" else 2**32
        if type(row[field]) is not int or not -limit <= row[field] < limit:
            raise ValueError(f"{path}: invalid integer {row[field]}")
        if row["event_id"] in ids:
            raise ValueError(f"{path}: duplicate id {row['event_id']}")
        ids.add(row["event_id"])
    if not rows:
        raise ValueError(f"{path}: empty dataset")
    return rows


def load(path):
    config = decode(path.read_bytes())
    if type(config.get("schema_version")) is not int or config["schema_version"] != 1:
        raise ValueError("schema_version must be 1")
    if config.get("protocol_version") != "jsonl-v1" or config.get("target") != "process":
        raise ValueError("expected jsonl-v1 and process target")
    command = config.get("command")
    if not isinstance(command, list) or not command or any(
        not isinstance(arg, str) or not arg or "\0" in arg for arg in command
    ):
        raise ValueError("command must be a nonempty argument array")
    mode = config.get("mode")
    if mode not in ("normal", "restart_before_result"):
        raise ValueError("invalid mode")
    timeout = config.get("timeouts_s")
    if not isinstance(timeout, dict) or set(timeout) != {"ready", "stage", "result", "exit", "total"}:
        raise ValueError("timeouts_s must contain ready, stage, result, exit and total")
    for value in timeout.values():
        if type(value) not in (int, float) or not 0 < value <= 300 or not math.isfinite(value):
            raise ValueError("timeouts must be seconds in (0, 300]")
    for key in ("input", "expected"):
        if not isinstance(config.get(key), str) or not Path(config[key]).is_file():
            raise ValueError(f"{key} must point to an existing file from project root")
    events = read_records(Path(config["input"]), "value")
    expected = read_records(Path(config["expected"]), "result")
    ids = {row["event_id"] for row in events}
    if ids != {row["event_id"] for row in expected}:
        raise ValueError("input and expected ID sets differ")
    fault = config.get("fault")
    if mode == "normal":
        if fault is not None:
            raise ValueError("normal mode cannot specify a fault")
    elif (
        not isinstance(fault, dict)
        or set(fault) - {"action_id"} != {"event_id", "stage", "max_restarts"}
        or not event_id(fault.get("action_id", "f1"))
        or not event_id(fault["event_id"])
        or fault["event_id"] not in ids
        or fault["stage"] != "before_result"
        or type(fault["max_restarts"]) is not int
        or fault["max_restarts"] != 1
    ):
        raise ValueError("fault must identify an input, before_result and one restart")
    return config, events, expected


class Run:
    def __init__(self, out, config, journal, observed):
        self.out = out
        self.config = config
        self.journal = journal
        self.observed = observed
        self.deadline = time.monotonic() + config["timeouts_s"]["total"]
        self.attempt = 0
        self.seq = 0
        self.faults = 0
        self.sent = 0
        self.results = []
        self.proc = None

    def log(self, kind, **details):
        self.seq += 1
        self.journal.write(encode({
            "seq": self.seq, "run_id": self.out.name, "attempt": self.attempt,
            "event_type": kind, "monotonic_ns": time.monotonic_ns(), "utc_ts": utc(),
            **details,
        }))
        self.journal.flush()

    def until(self, phase):
        return min(self.deadline, time.monotonic() + self.config["timeouts_s"][phase])

    def timeout(self, phase):
        if time.monotonic() >= self.deadline:
            return RunError("total_timeout", f"total timeout during {phase}")
        code = "trigger_not_reached" if phase == "stage" else f"{phase}_timeout"
        return RunError(code, f"deadline expired waiting for {phase}")

    def start(self):
        attempt = self.attempt + 1
        with (self.out / f"stderr-{attempt}.log").open("wb") as stderr:
            try:
                self.proc = subprocess.Popen(
                    self.config["command"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=stderr,
                )
            except OSError as exc:
                raise RunError("startup_failure", str(exc)) from exc
        self.attempt = attempt
        self.raw = (self.out / f"stdout-{attempt}.log").open("wb")
        self.buffer = bytearray()
        os.set_blocking(self.proc.stdin.fileno(), False)
        os.set_blocking(self.proc.stdout.fileno(), False)
        self.log("process_started", pid=self.proc.pid)
        if self.receive("ready", self.until("ready")) != {"type": "ready"}:
            raise RunError("protocol_error", "expected ready")
        self.log("ready", pid=self.proc.pid)

    def send(self, row, phase):
        deadline = self.until(phase)
        fd = self.proc.stdin.fileno()
        payload = encode(row).encode("utf-8")
        if not select.select([], [fd], [], max(0, deadline - time.monotonic()))[1]:
            raise self.timeout(phase)
        try:
            if os.write(fd, payload) != len(payload):
                raise RunError("write_error", "partial write to target")
        except BrokenPipeError as exc:
            raise RunError("unexpected_exit", "target closed stdin") from exc
        self.log("message_sent", message=row)

    def read(self, phase, deadline, allow_eof=False):
        fd = self.proc.stdout.fileno()
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
                raise self.timeout(phase)
            data = os.read(fd, 4096)
            self.raw.write(data)
            self.raw.flush()
            if not data:
                if self.buffer:
                    raise RunError("protocol_error", "truncated JSONL record")
                if allow_eof:
                    return None
                try:
                    code = self.proc.wait(timeout=min(0.2, max(0, deadline - time.monotonic())))
                except subprocess.TimeoutExpired as exc:
                    raise RunError("protocol_error", f"target closed stdout while waiting for {phase}") from exc
                self.log("process_exit", pid=self.proc.pid, returncode=code, planned=False)
                raise RunError("unexpected_exit", f"target exited with {code} while waiting for {phase}")
            self.buffer.extend(data)
            if len(self.buffer) > 8192:
                raise RunError("protocol_error", "stdout record exceeds limit")
        line, _, rest = self.buffer.partition(b"\n")
        self.buffer = rest
        try:
            return decode(line)
        except (ValueError, UnicodeError) as exc:
            raise RunError("protocol_error", f"invalid JSONL: {exc}") from exc

    def receive(self, phase, deadline, allow_eof=False):
        row = self.read(phase, deadline, allow_eof)
        if row is not None and row.get("type") == "result":
            if (
                set(row) != {"type", "event_id", "result"}
                or not event_id(row["event_id"])
                or type(row["result"]) is not int
            ):
                raise RunError("protocol_error", f"invalid result {row}")
            result = {"attempt": self.attempt, "event_id": row["event_id"], "result": row["result"]}
            self.results.append(result)
            self.observed.write(encode(result))
            self.observed.flush()
        if row is not None:
            self.log("message_received", message=row)
        return row

    def stop(self, killed):
        deadline = self.until("exit")
        if killed:
            pid = self.proc.pid
            fault = self.config["fault"]
            action_id = fault.get("action_id", "f1")
            action = {"action_id": action_id, "fault_id": action_id,
                      "event_id": fault["event_id"], "pid": pid}
            if self.proc.poll() is not None:
                raise RunError("unexpected_exit", "target exited before fault")
            self.proc.send_signal(signal.SIGKILL)
            self.log("fault_sent", signal="SIGKILL", **action)
        else:
            self.send({"type": "finish"}, "exit")
            self.proc.stdin.close()
        while (row := self.receive("exit", deadline, allow_eof=True)) is not None:
            # Retain extra results for the validator. During a crash drain they
            # would violate the barrier proof and make replay unsafe.
            if killed or row.get("type") != "result":
                raise RunError("protocol_error", f"unexpected message during exit: {row}")
        try:
            code = self.proc.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise self.timeout("exit") from exc
        self.log("process_exit", pid=self.proc.pid, returncode=code, planned=killed)
        if code != (-signal.SIGKILL if killed else 0):
            raise RunError("unexpected_exit", f"unexpected exit code {code}")
        if killed:
            self.faults += 1
            self.log("fault_confirmed", **action)
        self.close()

    def close(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=3)
            self.log("cleanup_exit", pid=self.proc.pid, returncode=self.proc.returncode)
        if not self.proc.stdout.closed:
            # Keep any unread raw bytes after an error, without waiting on EOF.
            for _ in range(16):
                data = self.proc.stdout.read(4096)
                if not data:
                    break
                self.raw.write(data)
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.raw.close()
        self.proc = None

    def process(self, events):
        self.start()
        fault = self.config.get("fault")
        for event in events:
            ident = event["event_id"]
            while True:
                armed = fault and fault["event_id"] == ident and self.faults == 0
                self.send({"type": "event", **event}, "result")
                self.sent += 1
                self.log("event_sent", event_id=ident)
                phase = "stage" if armed else "result"
                row = self.receive(phase, self.until(phase))
                stage = {"type": "stage", "name": "before_result", "event_id": ident}
                if row == stage:
                    self.log("stage", event_id=ident, name="before_result")
                    if armed:
                        self.stop(killed=True)
                        self.start()
                        continue
                    self.send({"type": "continue", "event_id": ident}, "result")
                    row = self.receive("result", self.until("result"))
                elif armed:
                    raise RunError("protocol_error", f"expected fault stage, got {row}")
                if row.get("type") != "result" or row["event_id"] != ident:
                    raise RunError("protocol_error", f"expected result for {ident}, got {row}")
                self.log("result", event_id=ident)
                break
        self.stop(killed=False)


def run(scenario, out):
    config, events, expected = load(scenario)
    if sys.platform != "linux":
        raise ValueError("Linux is required for pipe deadlines and SIGKILL")
    out.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(scenario, out / "scenario.json")
    for key in ("input", "expected"):
        shutil.copyfile(config[key], out / f"{key}.jsonl")
    save(out / "manifest.json", {
        "run_id": out.name, "started_utc": utc(), "os": platform.platform(),
        "python": sys.version, "protocol_version": config["protocol_version"],
        "target_command": config["command"], "cwd": str(Path.cwd()),
        "runner_command": [sys.executable, "-m", "pgval.executor", *sys.argv[1:]],
        "input_sha256": hashlib.sha256((out / "input.jsonl").read_bytes()).hexdigest(),
        "expected_source": f"manually prepared reference {config['expected']}",
    })
    with (out / "journal.jsonl").open("w", encoding="utf-8") as journal, (
        out / "observed.jsonl"
    ).open("w", encoding="utf-8") as observed:
        execution = Run(out, config, journal, observed)
        summary = {"execution_status": "completed", "reason_code": None}
        try:
            execution.process(events)
        except (RunError, OSError, KeyboardInterrupt) as exc:
            summary.update(
                execution_status="failed", reason_code=getattr(exc, "code", "interrupted" if isinstance(exc, KeyboardInterrupt) else "io_error"),
                error=str(exc),
            )
            execution.log("error", reason_code=summary["reason_code"], error=str(exc))
        finally:
            try:
                execution.close()
            except (OSError, subprocess.TimeoutExpired) as exc:
                summary.update(execution_status="failed", reason_code="cleanup_error", error=str(exc))
                execution.log("error", reason_code="cleanup_error", error=str(exc))
        counts = compare(expected, execution.results)
        if summary["execution_status"] != "completed":
            counts["validation_status"] = "not_run"
        summary.update(counts, attempts=execution.attempt, confirmed_faults=execution.faults,
                       sent=execution.sent, expected_order_observed=summary["execution_status"] == "completed",
                       evidence=["scenario.json", "input.jsonl", "expected.jsonl", "observed.jsonl", "journal.jsonl"])
    success = summary["execution_status"] == "completed" and summary["validation_status"] == "passed"
    summary["exit_code"] = 0 if success else 1
    save(out / "summary.json", summary)
    print(encode(summary), end="")
    return summary["exit_code"]


def main():
    parser = argparse.ArgumentParser()
    command = parser.add_subparsers(dest="action", required=True).add_parser("run")
    command.add_argument("--scenario", type=Path, required=True)
    command.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        return run(args.scenario, args.out)
    except (OSError, ValueError) as exc:
        print(f"configuration/output error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

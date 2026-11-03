#!/usr/bin/env python3
"""Temporary stand-in for the team's data generator (to be replaced).

Prints click events for the SST job as "key<TAB>value" lines, ready for
kafka-console-producer with parse.key=true. Mirrors the author's KafkaDataGen
record shape: key is the JSON-encoded page name, value is a ClickEvent.

Timestamps increase monotonically (the SST job uses forMonotonousTimestamps),
ids are unique, and final events one window later (one per page, so one per
partition) close the last window.
The same --seed always produces the same events.
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone

PAGES = ["/help", "/index", "/shop", "/jobs", "/about", "/news"]
WINDOW = timedelta(seconds=60)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pages", type=int, default=6, help="number of pages (max 6)")
    p.add_argument("--events-per-window", type=int, default=100, help="events per page per window")
    p.add_argument("--windows", type=int, default=5)
    p.add_argument("--rate", type=float, default=500, help="events per second of wall clock, 0 = no limit")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--start", default="2026-11-03T10:00:00", help="event time of the first window (UTC)")
    a = p.parse_args()

    rnd = random.Random(a.seed)
    pages = PAGES[: a.pages]
    start = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
    per_window = a.events_per_window * len(pages)
    step = WINDOW / per_window  # even spacing keeps timestamps strictly increasing

    events = []
    for w in range(a.windows):
        window_pages = [pg for pg in pages for _ in range(a.events_per_window)]
        rnd.shuffle(window_pages)
        for i, page in enumerate(window_pages):
            events.append((start + w * WINDOW + i * step, page))
    # Close the last window: one event per page one window later, so that every
    # partition (keyed by page) advances its watermark, not just one.
    closing = start + (a.windows + 1) * WINDOW
    events += [(closing + k * timedelta(milliseconds=1), page) for k, page in enumerate(pages)]

    for n, (ts, page) in enumerate(events, start=1):
        value = {"id": n, "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}", "page": page}
        print(f"{json.dumps(page)}\t{json.dumps(value, separators=(',', ':'))}", flush=True)
        if a.rate > 0:
            time.sleep(1 / a.rate)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(1)

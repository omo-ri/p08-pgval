"""Compare independently supplied expected records with observed records."""

from collections import Counter


def compare(expected, observed):
    want = Counter((row["event_id"], row["result"]) for row in expected)
    got = Counter((row["event_id"], row["result"]) for row in observed)
    missing = want - got
    extra = got - want
    substitutions = Counter()
    unexpected_ids = Counter()
    for (event_id, _), count in missing.items():
        substitutions[event_id] += count
    for (event_id, _), count in extra.items():
        unexpected_ids[event_id] += count
    mismatch = sum((substitutions & unexpected_ids).values())
    return {
        "received": got.total(),
        "missing": missing.total() - mismatch,
        "extra": extra.total() - mismatch,
        "mismatched": mismatch,
        "validation_status": "passed" if want == got else "failed",
    }

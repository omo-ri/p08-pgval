"""Independent offline Oracle for PGVal Flink SST; legacy KStreams fixtures retained.
Supports unique IDs, monotonic event time, and final PageStatistics outputs only.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import json
from pathlib import Path
from pgval.plan import planned_events

EPOCH = datetime(1970, 1, 1)
FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def read(path):
    return json.loads(Path(path).read_text())


def read_records(path):
    """Read JSON arrays, JSONL values or Kafka console producer key<TAB>value."""
    text = Path(path).read_text(encoding='utf-8')
    if text.lstrip().startswith('['):
        rows = json.loads(text)
    else:
        rows = []
        for number, line in enumerate(text.splitlines(), 1):
            if not line.strip():
                continue
            try:
                if '\t' in line:
                    key, value = line.split('\t', 1)
                    row = json.loads(value)
                    if json.loads(key) != row.get('page'):
                        raise ValueError('Kafka key differs from page')
                else:
                    row = json.loads(line)
                rows.append(row)
            except (ValueError, TypeError, AttributeError) as error:
                raise ValueError(f'{path}:{number}: {error}') from error
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError(f'{path}: expected records to be JSON objects')
    return rows


def timestamp(value):
    if type(value) is int:
        return value
    if not isinstance(value, str):
        raise ValueError('timestamp must be integer milliseconds or an ISO date string')
    # Upstream Jackson format has no timezone; smoke harness uses UTC explicitly.
    delta = datetime.strptime(value, FORMAT) - EPOCH
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def formatted(value):
    return (EPOCH + timedelta(milliseconds=value)).isoformat(timespec='milliseconds')


def check_plan(plan):
    topology = plan['topology']
    if plan['window_ms'] != 60000:
        raise ValueError('Pinned SST uses 60s windows')
    if topology == 'pgval_flink_sst':
        if plan.get('allowed_lateness_ms') != 0 or plan.get('processing_guarantee') not in ('a1', 'e1'):
            raise ValueError('Flink SST requires zero allowed lateness and explicit a1/e1')
    elif topology == 'pgval_kstreams_sst':
        if plan.get('grace_ms') != 2000:
            raise ValueError('Legacy KStreams SST requires 2s grace')
    else:
        raise ValueError('Unsupported topology')
    end = plan['evaluate_until_ms']
    if type(end) is not int or end <= 0 or end % plan['window_ms']:
        raise ValueError('evaluation horizon must be a positive window boundary')


def oracle(events, plan):
    check_plan(plan)
    groups = defaultdict(list)
    seen, last = set(), -1
    for event in events:
        identifier = event['id']
        if type(identifier) is not int or identifier in seen:
            raise ValueError('Unique integer input IDs required; duplicate publication semantics not supported')
        seen.add(identifier)
        time = timestamp(event['timestamp'])
        if time < last or time < 0:
            raise ValueError('This smoke case requires nonnegative monotonic event time')
        last = time
        if not isinstance(event['page'], str):
            raise ValueError('page must be a string')
        if time >= plan['evaluate_until_ms']:
            continue
        start = time // plan['window_ms'] * plan['window_ms']
        groups[(event['page'], start)].append(identifier)
    if last <= plan['evaluate_until_ms'] + plan.get('grace_ms', 0):
        raise ValueError('Need tail event beyond evaluation horizon (plus grace for legacy KStreams); actual watermark/commit completion must be confirmed externally')
    return [dict(page=page, windowStart=formatted(start),
                 windowEnd=formatted(start + plan['window_ms']),
                 clickIds=sorted(ids), updateIds=[])
            for (page, start), ids in sorted(groups.items())]


def identity(row):
    if not isinstance(row['page'], str):
        raise ValueError('page must be a string')
    for field in ('clickIds', 'updateIds'):
        if not isinstance(row[field], list) or any(type(i) is not int for i in row[field]):
            raise ValueError(f'{field} must be a list of integers')
    return row['page'], timestamp(row['windowStart']), timestamp(row['windowEnd'])


def validate(expected, actual, complete, guarantee='e1'):
    if guarantee not in ('a1', 'e1'):
        raise ValueError('Unsupported guarantee')
    exp = {identity(row): row for row in expected}
    groups = defaultdict(list)
    for row in actual:
        groups[identity(row)].append(row)
    issues, correct, missing, extra = [], 0, 0, 0
    duplicate_only_keys = 0
    for key in sorted(exp.keys() | groups.keys()):
        target, rows = exp.get(key), groups[key]
        want = Counter(target['clickIds']) if target else Counter()
        got = Counter(i for row in rows for i in row['clickIds'])
        absent, surplus = want - got, got - want
        missing += sum(absent.values())
        extra += sum(surplus.values())
        errors = []
        if not target:
            errors.append('unexpected_result')
        if target and not rows:
            errors.append('missing_result' if complete else 'pending_result')
        if len(rows) > 1:
            errors.append('duplicate_result')
        if absent:
            errors.append('missing_ids' if complete else 'pending_ids')
        if surplus:
            errors.append('extra_ids')
        if any(row['updateIds'] for row in rows):
            errors.append('unexpected_update_ids_in_sst')
        duplicate_only = (target is not None and len(rows) > 1
                          and all(Counter(row['clickIds']) == want and not row['updateIds'] for row in rows))
        if duplicate_only:
            duplicate_only_keys += 1
        if errors:
            issues.append(dict(result_key=list(key), errors=errors,
                               missing_ids=dict(absent), extra_ids=dict(surplus)))
        else:
            correct += 1
    return dict(status=('pass' if (not issues or (guarantee == 'a1' and len(issues) == duplicate_only_keys)) else 'fail') if complete else 'incomplete',
                processing_guarantee=guarantee,
                strict_output_match=not issues if complete else None,
                permitted_repeated_results=duplicate_only_keys if guarantee == 'a1' else 0,
                expected_results=len(exp), observed_results=len(actual), correct_results=correct,
                correct_result_fraction=correct / len(exp) if exp else None,
                missing_contributions=missing if complete else None, extra_contributions=extra,
                issues=issues,
                limitation='Validates observable SST outputs only; upstream deduplicates IDs inside aggregates. No end-to-end fault or exactly-once proof.')


def main():
    parser = argparse.ArgumentParser()
    for name in ('plan', 'outputs', 'report'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--inputs', help='Optional submission log for v2; required for legacy v1')
    parser.add_argument('--observed-inputs', help='Kafka click JSONL; compare to the ordered producer input without assuming cross-partition order')
    parser.add_argument('--late-outputs', help='lateOutput JSONL; baseline workload expects no late events')
    parser.add_argument('--complete', action='store_true', help='Assert finalization and collection are complete; not inferred from logs')
    args = parser.parse_args()
    try:
        plan = read(args.plan)
        version = plan.get('schema_version', 1)
        canonical = lambda rows: Counter(json.dumps(row, sort_keys=True) for row in rows)
        if version == 2:
            inputs = planned_events(plan)
            if args.inputs and canonical(inputs) != canonical(read_records(args.inputs)):
                raise ValueError('Submission log differs from planned publications')
        elif version == 1 and args.inputs:
            inputs = read_records(args.inputs)
        else:
            raise ValueError('Unsupported plan version or missing --inputs for legacy v1')
        if args.observed_inputs:
            observed = read_records(args.observed_inputs)
            canonical = lambda rows: Counter(json.dumps(row, sort_keys=True) for row in rows)
            if canonical(inputs) != canonical(observed):
                raise ValueError('Broker-observed input differs from submitted input; reconcile delivery before evaluating application output')
        expected = oracle(inputs, plan)
        result = validate(expected, read_records(args.outputs), args.complete, plan.get('processing_guarantee', 'e1'))
        if args.late_outputs:
            late = read_records(args.late_outputs)
            result['late_records'] = len(late)
            if late:
                result['issues'].append(dict(errors=['unexpected_late_output'], records=late))
                result['strict_output_match'] = False if args.complete else None
                result['status'] = 'fail' if args.complete else 'incomplete'
        else:
            result['late_records'] = None
        result['fault_execution'] = ('unspecified_legacy_plan' if version == 1 else
                                     'not_checked' if any(a.get('type') == 'fault' for a in plan['actions']) else 'not_requested')
        result['scope'] = 'observable main output; no fault execution verification'
        result['sources'] = dict(plan=args.plan, inputs=args.inputs, outputs=args.outputs,
                                 late_outputs=args.late_outputs, observed_inputs=args.observed_inputs)
    except (ValueError, KeyError, TypeError, OSError, OverflowError) as error:
        result = dict(status='invalid_input', error=str(error))
    Path(args.report).write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['status'] == 'pass' else 1

if __name__ == '__main__':
    raise SystemExit(main())

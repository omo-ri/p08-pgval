"""Generate a self-contained PGVal SST action plan. No Oracle dependency."""
import argparse
from datetime import datetime, timedelta
import json
from pathlib import Path
from copy import deepcopy
from pgval.plan import planned_events
if __package__:
    from .generators import Config, GENERATORS, get_generator
else:
    from generators import Config, GENERATORS, get_generator

REVISION = 'd74c9b80a3a196c4ccff38eed3d77017db0c6ee0'


def encode(value):
    return json.dumps(value, ensure_ascii=False, indent=2) + '\n'


def generate(seed=42, windows=2, pages=2, events_per_window=10,
             generator='sst-random', window_ms=60000, guarantee='e1', rate=100, faults=None):
    if guarantee not in ('a1', 'e1'):
        raise ValueError('guarantee must be a1 or e1')
    config = Config(seed, windows, pages, events_per_window, window_ms)
    config.validate()
    plugin = get_generator(generator)
    events = []
    def add(time_ms, page):
        date = (datetime(1970, 1, 1) + timedelta(milliseconds=time_ms)).isoformat(timespec='milliseconds')
        events.append(dict(id=len(events) + 1, timestamp=date, page=page))
    allowed_pages = {f'/page-{i + 1}' for i in range(pages)}
    previous = -1
    counts = [0] * windows
    for time_ms, page in plugin.events(config):
        if type(time_ms) is not int or not previous <= time_ms < windows * window_ms or time_ms < 0:
            raise ValueError('Generator must emit ordered times inside the evaluation horizon')
        if page not in allowed_pages:
            raise ValueError('Generator emitted a page outside the configured set')
        counts[time_ms // window_ms] += 1
        previous = time_ms
        add(time_ms, page)
    if any(count != events_per_window for count in counts):
        raise ValueError('Generator did not emit the configured event count per window')
    horizon = windows * window_ms
    add(horizon + 1, '/__advance__')
    plan = dict(schema_version=2, topology='pgval_flink_sst', upstream_commit=REVISION,
                window_ms=window_ms, allowed_lateness_ms=0, processing_guarantee=guarantee, evaluate_until_ms=horizon,
                input_policy='unique_ids_monotonic_event_time',
                execution='external Flink App.java with Kafka; executor required',
                system=dict(engine='flink', version='1.17.1', connector_version='1.17.1',
                            kafka_broker_version='3.7.0', java_version='11'),
                generator=dict(name=plugin.name, version=plugin.version, seed=seed, windows=windows,
                               pages=pages, events_per_window=events_per_window),
                input=dict(topic='click', key_field='page'),
                completion=dict(type='advance_watermarks_all_partitions', event_id=events[-1]['id'],
                                event_time_ms=horizon+1, evaluate_tail_window=False, requires_executor_confirmation=True,
                                note='For multiple partitions send tail events with unique IDs to every partition and record actual inputs; await watermark progress and committed output.'),
                output=dict(topic='output', format='PageStatistics', final_only=True, isolation_level='read_committed',
                            late_topic='lateOutput'))
    plan['actions'] = [dict(id='publish-main', type='publish',
                            trigger=dict(type='test_start', delay_ms=0), topic='click',
                            rate_per_second=rate, events=deepcopy(events[:-1]))]
    if faults is not None:
        if not isinstance(faults, list) or any(not isinstance(f, dict) or f.get('type') != 'fault' for f in faults):
            raise ValueError('faults must be a list of fault actions')
        plan['actions'].extend(deepcopy(faults))
    plan['actions'].append(dict(id='publish-tail', type='publish',
                                trigger=dict(type='action', action_id='publish-main', phase='completed', delay_ms=0),
                                topic='click', rate_per_second=rate, events=deepcopy(events[-1:])))
    planned_events(plan)
    return plan, events


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out-dir')
    parser.add_argument('--list-generators', action='store_true')
    parser.add_argument('--generator', choices=sorted(GENERATORS), default='sst-random')
    parser.add_argument('--window-ms', type=int, default=60000,
                        help='Pinned baseline supports 60000 only')
    parser.add_argument('--guarantee', choices=('a1', 'e1'), default='e1')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--windows', type=int, default=2)
    parser.add_argument('--pages', type=int, default=2)
    parser.add_argument('--events-per-window', type=int, default=10,
                        help='Total data events per window across all pages')
    parser.add_argument('--rate', type=float, default=100, help='Planned publications per second')
    parser.add_argument('--faults-json', help='JSON list of executor-defined fault actions, embedded into the plan')
    args = parser.parse_args()
    if args.list_generators:
        for plugin in GENERATORS.values():
            print(f'{plugin.name}: {plugin.description}')
        return
    if not args.out_dir:
        parser.error('--out-dir is required unless --list-generators is used')
    try:
        plan, events = generate(args.seed, args.windows, args.pages, args.events_per_window,
                                args.generator, args.window_ms, args.guarantee, args.rate,
                                json.loads(Path(args.faults_json).read_text()) if args.faults_json else None)
        destination = Path(args.out_dir)
        # A new directory avoids overwriting an existing experiment accidentally.
        destination.mkdir(parents=True, exist_ok=False)
        (destination / 'plan.json').write_text(encode(plan), encoding='utf-8')
    except (ValueError, OSError) as error:
        parser.exit(2, f'{error}\n')
    print(f'Created {destination}/plan.json: {len(events)-1} data events + 1 tail event')


if __name__ == '__main__':
    main()

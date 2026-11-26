"""Shared plan structure only. No application or Oracle calculations."""
import math


def _number(value, name, positive=False):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f'{name} must be a finite {"positive" if positive else "nonnegative"} number')


def planned_events(plan):
    """Validate v2 actions and extract intended publications in declared order."""
    if plan.get('schema_version') != 2:
        raise ValueError('Action plans require schema_version=2')
    actions = plan.get('actions')
    if not isinstance(actions, list) or not actions:
        raise ValueError('actions must be a nonempty list')
    known, events, last_publish = {}, [], None
    for action in actions:
        if not isinstance(action, dict):
            raise ValueError('action must be an object')
        identifier = action.get('id')
        if not isinstance(identifier, str) or not identifier or identifier in known:
            raise ValueError('action IDs must be unique nonempty strings')
        trigger = action.get('trigger', {})
        if not isinstance(trigger, dict):
            raise ValueError('trigger must be an object')
        kind = trigger.get('type')
        if kind == 'test_start':
            pass
        elif kind == 'action':
            if trigger.get('action_id') not in known or trigger.get('phase') not in ('started', 'completed'):
                raise ValueError('action trigger must reference an earlier action and started/completed phase')
        elif kind == 'signal':
            if not isinstance(trigger.get('name'), str) or not trigger['name']:
                raise ValueError('signal name is required')
            _number(trigger.get('timeout_ms'), 'signal timeout_ms', positive=True)
        else:
            raise ValueError('unknown trigger type')
        _number(trigger.get('delay_ms', 0), 'delay_ms')
        operation = action.get('type')
        if operation == 'publish':
            # Serial publications give the SST Oracle a defined input order.
            if last_publish and not (kind == 'action' and trigger.get('action_id') == last_publish and trigger.get('phase') == 'completed'):
                raise ValueError('publish actions must follow completion of the previous publish action')
            if not last_publish and kind != 'test_start':
                raise ValueError('first publish must trigger at test_start')
            if action.get('topic') != plan['input']['topic']:
                raise ValueError('publish topic differs from input topic')
            _number(action.get('rate_per_second'), 'rate_per_second', positive=True)
            records = action.get('events')
            if not isinstance(records, list) or not records:
                raise ValueError('publish events must be a nonempty list')
            for event in records:
                if not isinstance(event, dict) or set(event) != {'id', 'page', 'timestamp'}:
                    raise ValueError('event must contain exactly id, page, timestamp')
                if type(event['id']) is not int or not isinstance(event['page'], str) or not isinstance(event['timestamp'], str):
                    raise ValueError('invalid event field type')
            events.extend(records)
            last_publish = identifier
        elif operation == 'fault':
            for field in ('scenario', 'target'):
                if not isinstance(action.get(field), str) or not action[field]:
                    raise ValueError(f'fault {field} is required')
            _number(action.get('duration_ms'), 'duration_ms', positive=True)
            if not isinstance(action.get('parameters', {}), dict):
                raise ValueError('fault parameters must be an object')
        else:
            raise ValueError(f'unknown action type: {operation}')
        known[identifier] = action
    if not events:
        raise ValueError('plan must publish events')
    return events
